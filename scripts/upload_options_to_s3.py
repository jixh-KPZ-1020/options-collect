#!/usr/bin/env python3
"""Convert Databento options data (.dbn.zst) to Parquet and upload to S3.

Usage:
    # Dry run (single date, convert only, no S3 upload)
    python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip --dry-run --dates 2025-08-07

    # Full pipeline
    python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip

    # Resume after interruption (automatically skips completed dates)
    python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip

    # Validate after full run
    python scripts/upload_options_to_s3.py data/raw/databento/OPRA-*.zip --validate

Requires: pip install -e ".[pipeline]"
Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME (in .env or exported)
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

# Add scripts/ to path so _options_pipeline is importable
sys.path.insert(0, str(Path(__file__).parent))

from _options_pipeline.config import PipelineConfig
from _options_pipeline.converter import convert_dbn_to_parquet
from _options_pipeline.extractor import (
    extract_dbn_file,
    list_dbn_files,
    load_manifest_hashes,
    parse_date_from_filename,
    verify_file_hash,
)
from _options_pipeline.preflight import PreflightError, run_preflight_checks
from _options_pipeline.shutdown import GracefulShutdown
from _options_pipeline.state import PipelineState
from _options_pipeline.uploader import build_s3_key, upload_to_s3
from _options_pipeline.validator import print_validation_report, validate_s3_dataset


def main() -> None:
    args = parse_args()

    # Load .env if python-dotenv is available
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    config = PipelineConfig.from_env(args.zip_path)

    if args.validate:
        run_validation(config)
        return

    run_pipeline(
        config=config,
        dry_run=args.dry_run,
        resume=not args.no_resume,
        date_filter=_parse_dates(args.dates) if args.dates else None,
        no_cleanup=args.no_cleanup,
    )


def run_pipeline(
    config: PipelineConfig,
    dry_run: bool = False,
    resume: bool = True,
    date_filter: set[str] | None = None,
    no_cleanup: bool = False,
) -> None:
    """Main pipeline: extract -> convert -> upload for each daily file."""
    # --- Pre-flight ---
    print("Running pre-flight checks...")
    try:
        results = run_preflight_checks(config, dry_run=dry_run)
    except PreflightError as e:
        print(f"\n{e}", file=sys.stderr)
        sys.exit(1)

    print(f"  ZIP OK: {results.get('total_dbn_files', '?')} daily files found")
    print(f"  Disk free: {results.get('disk_free_gb', '?')} GB")
    if results.get("previously_completed"):
        print(f"  Resuming: {results['previously_completed']} already done, "
              f"{results.get('remaining', '?')} remaining")
    print()

    # --- State ---
    state = PipelineState(config.state_file)
    if resume:
        state.load()

    dbn_files = list_dbn_files(config.zip_path)
    manifest_hashes = load_manifest_hashes(config.zip_path)

    # Extract job ID from ZIP name
    job_id = config.zip_path.stem
    state.init_job(job_id, str(config.zip_path), config.bucket_name, config.s3_prefix)

    # Filter files
    pending = state.pending_files(dbn_files)
    if date_filter:
        pending = [
            f for f in pending
            if parse_date_from_filename(f).isoformat() in date_filter
        ]

    total = len(pending)
    if total == 0:
        print("Nothing to process. All files are already completed.")
        return

    # --- Print summary ---
    mode = "DRY RUN" if dry_run else "UPLOAD"
    print(f"Pipeline [{mode}]")
    print(f"  ZIP: {config.zip_path.name}")
    print(f"  Files to process: {total}")
    if not dry_run:
        print(f"  Target: s3://{config.bucket_name}/{config.s3_prefix}/")
    print()

    # --- Signal handling ---
    shutdown = GracefulShutdown()
    shutdown.install()

    # --- Main loop ---
    processed = 0
    failed = 0

    try:
        for i, filename in enumerate(pending, 1):
            if shutdown.requested:
                print(f"\nGraceful shutdown: {i-1}/{total} processed, "
                      f"{total - i + 1} remaining")
                break

            trade_date = parse_date_from_filename(filename)
            print(f"[{i}/{total}] {trade_date.isoformat()} ({filename})")

            try:
                _process_single_file(
                    filename=filename,
                    trade_date=trade_date,
                    config=config,
                    state=state,
                    manifest_hashes=manifest_hashes,
                    dry_run=dry_run,
                    no_cleanup=no_cleanup,
                )
                processed += 1
            except Exception as e:
                state.mark_failed(filename, str(e))
                print(f"  [ERROR] {e}", file=sys.stderr)
                failed += 1
                continue

    finally:
        shutdown.uninstall()

    # --- Summary ---
    print(f"\n{'='*50}")
    print(f"Pipeline complete.")
    summary = state.summary()
    print(f"  Completed: {summary['completed']}/{len(dbn_files)}")
    print(f"  Failed:    {summary['failed']}")
    print(f"  This run:  {processed} processed, {failed} failed")
    if summary["failed"] > 0:
        print(f"\nRe-run the same command to retry failed files.")


def _process_single_file(
    filename: str,
    trade_date,
    config: PipelineConfig,
    state: PipelineState,
    manifest_hashes: dict[str, str],
    dry_run: bool,
    no_cleanup: bool,
) -> None:
    """Extract, convert, upload a single daily file.

    All intermediate files are in a TemporaryDirectory that is
    automatically cleaned up on any exit path.
    """
    # Use same filesystem as ZIP for temp dir (avoid cross-device issues)
    work_base = config.zip_path.parent
    cleanup = not no_cleanup

    with tempfile.TemporaryDirectory(
        dir=work_base,
        prefix=f".pipeline_{trade_date.isoformat()}_",
    ) as tmp_dir:
        tmp_path = Path(tmp_dir)
        dbn_path = tmp_path / filename
        parquet_path = tmp_path / "data.parquet"

        # Step 1: Extract from ZIP
        print(f"  Extracting...")
        extract_dbn_file(config.zip_path, filename, dbn_path)

        # Step 2: Verify hash against manifest
        if filename in manifest_hashes:
            verify_file_hash(dbn_path, manifest_hashes[filename])
            print(f"  Hash verified against manifest")

        # Step 3: Convert DBN -> Parquet
        print(f"  Converting to Parquet...")
        convert_dbn_to_parquet(dbn_path, parquet_path)
        parquet_size_mb = parquet_path.stat().st_size / (1024 * 1024)
        print(f"  Parquet: {parquet_size_mb:.1f} MB")

        # Free disk: delete .dbn.zst before upload
        if cleanup:
            dbn_path.unlink(missing_ok=True)

        # Step 4: Upload to S3 (or skip in dry-run)
        if dry_run:
            print(f"  [DRY RUN] Would upload to "
                  f"s3://{config.bucket_name}/"
                  f"{build_s3_key(config.s3_prefix, config.underlying_symbol, trade_date)}")
            state.mark_completed(
                filename,
                s3_key="(dry-run)",
                size_bytes=int(parquet_size_mb * 1024 * 1024),
            )
        else:
            s3_key = build_s3_key(config.s3_prefix, config.underlying_symbol, trade_date)
            print(f"  Uploading to S3...")
            result = upload_to_s3(
                parquet_path,
                config.bucket_name,
                s3_key,
                config.aws_region,
            )
            print(f"  -> s3://{config.bucket_name}/{s3_key}")

            state.mark_completed(
                filename,
                s3_key=s3_key,
                s3_etag=result["etag"],
                sha256=result["sha256"],
                size_bytes=result["size_bytes"],
            )

    # TemporaryDirectory cleaned up here


def run_validation(config: PipelineConfig) -> None:
    """Run post-pipeline validation against S3."""
    if not config.bucket_name:
        print("Error: S3_BUCKET_NAME not set", file=sys.stderr)
        sys.exit(1)
    if not config.state_file.exists():
        print(f"Error: State file not found: {config.state_file}", file=sys.stderr)
        sys.exit(1)

    print(f"Validating S3 dataset...")
    print(f"  Bucket: {config.bucket_name}")
    print(f"  Prefix: {config.s3_prefix}")
    print(f"  State:  {config.state_file}")
    print()

    report = validate_s3_dataset(
        bucket=config.bucket_name,
        s3_prefix=config.s3_prefix,
        state_path=config.state_file,
        zip_path=config.zip_path,
        region=config.aws_region,
    )
    print_validation_report(report)

    if report["overall"] != "pass":
        sys.exit(1)


def _parse_dates(dates_str: str) -> set[str]:
    """Parse comma-separated date strings."""
    return {d.strip() for d in dates_str.split(",") if d.strip()}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Databento options data to Parquet and upload to S3.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "zip_path",
        help="Path to the Databento ZIP file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Convert to Parquet locally but don't upload to S3",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start fresh, ignore previous pipeline state",
    )
    parser.add_argument(
        "--dates",
        help="Process only these dates (comma-separated YYYY-MM-DD)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep intermediate files (for debugging)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate S3 dataset after upload (no processing)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
