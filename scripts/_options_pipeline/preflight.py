"""Pre-flight validation checks — fail fast before a long pipeline run.

Validates: ZIP integrity, disk space, S3 access, databento availability,
state file integrity, and cleans up stale artifacts from crashed runs.
"""

from __future__ import annotations

import shutil
import zipfile
from pathlib import Path

from _options_pipeline.config import PipelineConfig
from _options_pipeline.state import PipelineState


class PreflightError(Exception):
    """Raised when a pre-flight check fails."""


def run_preflight_checks(config: PipelineConfig, dry_run: bool = False) -> dict:
    """Run all pre-flight checks before starting the pipeline.

    Returns a summary dict. Raises PreflightError on any critical failure.
    """
    results: dict = {}
    errors: list[str] = []

    # 1. ZIP file exists and is readable
    if not config.zip_path.exists():
        errors.append(f"ZIP file not found: {config.zip_path}")
    else:
        try:
            with zipfile.ZipFile(config.zip_path) as zf:
                dbn_files = [n for n in zf.namelist() if n.endswith(".dbn.zst")]
                results["total_dbn_files"] = len(dbn_files)
                results["zip_ok"] = True
        except zipfile.BadZipFile as e:
            errors.append(f"Invalid ZIP file: {e}")

    # 2. Disk space (need >= 500 MB for one extract + one parquet at peak)
    work_dir = config.zip_path.parent
    if work_dir.exists():
        usage = shutil.disk_usage(work_dir)
        free_gb = usage.free / (1024**3)
        results["disk_free_gb"] = round(free_gb, 2)
        if free_gb < 0.5:
            errors.append(f"Insufficient disk space: {free_gb:.1f} GB free, need >= 0.5 GB")

    # 3. databento library available
    try:
        import databento  # noqa: F401

        results["databento_available"] = True
    except ImportError:
        errors.append(
            'databento package not installed. Run: pip install -e ".[pipeline]"'
        )

    # 4. S3 access (skip in dry-run mode)
    if not dry_run:
        if not config.bucket_name:
            errors.append("S3_BUCKET_NAME not set in environment")
        else:
            try:
                import boto3

                s3 = boto3.client("s3", region_name=config.aws_region)
                s3.head_bucket(Bucket=config.bucket_name)
                results["s3_auth_ok"] = True

                # Test write permission
                test_key = f"{config.s3_prefix}/.preflight_test"
                s3.put_object(Bucket=config.bucket_name, Key=test_key, Body=b"preflight")
                s3.delete_object(Bucket=config.bucket_name, Key=test_key)
                results["s3_write_ok"] = True
            except ImportError:
                errors.append(
                    'boto3 package not installed. Run: pip install -e ".[pipeline]"'
                )
            except Exception as e:
                errors.append(f"S3 access failed: {e}")

    # 5. State file integrity (if resuming)
    if config.state_file.exists():
        try:
            import json

            json.loads(config.state_file.read_text())
            results["state_file_ok"] = True
        except Exception as e:
            errors.append(f"State file corrupt ({config.state_file}): {e}")

    # 6. Clean up stale temp directories from crashed runs
    stale = cleanup_stale_temp_dirs(config.zip_path.parent)
    if stale > 0:
        results["stale_dirs_cleaned"] = stale

    # 7. Resume summary
    if config.state_file.exists() and results.get("state_file_ok"):
        state = PipelineState(config.state_file)
        state.load()
        summary = state.summary()
        results["previously_completed"] = summary["completed"]
        results["previously_failed"] = summary["failed"]
        results["remaining"] = results.get("total_dbn_files", 0) - summary["completed"]

    if errors:
        raise PreflightError(
            "Pre-flight checks failed:\n" + "\n".join(f"  - {e}" for e in errors)
        )

    return results


def cleanup_stale_temp_dirs(base_dir: Path, prefix: str = ".pipeline_") -> int:
    """Remove leftover temp directories from crashed pipeline runs."""
    cleaned = 0
    if not base_dir.exists():
        return 0

    for item in base_dir.iterdir():
        if item.is_dir() and item.name.startswith(prefix):
            shutil.rmtree(item, ignore_errors=True)
            cleaned += 1
    return cleaned
