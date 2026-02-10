"""Integration test: full pipeline with mocked S3 and mocked databento conversion."""

from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.config import PipelineConfig
from _options_pipeline.state import PipelineState

BUCKET = "test-pipeline-bucket"
PREFIX = "options/cbbo-1m"


@pytest.fixture
def tiny_zip(tmp_path: Path) -> Path:
    """Create a ZIP with 3 fake daily .dbn.zst files + manifest."""
    zip_path = tmp_path / "OPRA-TEST-001.zip"
    manifest_files = []

    files = {}
    for day in [7, 8, 11]:
        name = f"opra-pillar-202508{day:02d}.cbbo-1m.dbn.zst"
        content = f"fake dbn data for day {day}".encode() * 100
        files[name] = content
        sha = hashlib.sha256(content).hexdigest()
        manifest_files.append({
            "filename": name,
            "size": len(content),
            "hash": f"sha256:{sha}",
        })

    manifest = {"job_id": "OPRA-TEST-001", "files": manifest_files}

    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
        zf.writestr("metadata.json", json.dumps({"job_id": "OPRA-TEST-001"}))
        zf.writestr("manifest.json", json.dumps(manifest))

    return zip_path


def _fake_convert(dbn_path: Path, parquet_path: Path) -> Path:
    """Mock converter that produces a small file with PAR1 magic bytes."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    # PAR1 magic at start and end (mimics valid Parquet)
    parquet_path.write_bytes(b"PAR1" + b"\x00" * 100 + b"PAR1")
    return parquet_path


@mock_aws
def test_full_pipeline_dry_run(tiny_zip: Path, tmp_path: Path):
    """Dry run processes all files without uploading to S3."""
    from scripts.upload_options_to_s3 import run_pipeline

    config = PipelineConfig(
        zip_path=tiny_zip,
        extract_dir=tmp_path / "extract",
        bucket_name="",  # No bucket needed for dry run
        s3_prefix=PREFIX,
        state_file=tmp_path / "state.json",
    )

    with patch(
        "scripts.upload_options_to_s3.convert_dbn_to_parquet",
        side_effect=_fake_convert,
    ):
        run_pipeline(config, dry_run=True, resume=False)

    # Check state file
    state = PipelineState(config.state_file)
    state.load()
    summary = state.summary()
    assert summary["completed"] == 3
    assert summary["failed"] == 0


@mock_aws
def test_full_pipeline_upload(tiny_zip: Path, tmp_path: Path):
    """Full pipeline uploads files to (mocked) S3."""
    from scripts.upload_options_to_s3 import run_pipeline

    # Create mock S3 bucket
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    config = PipelineConfig(
        zip_path=tiny_zip,
        extract_dir=tmp_path / "extract",
        bucket_name=BUCKET,
        s3_prefix=PREFIX,
        state_file=tmp_path / "state.json",
    )

    with patch(
        "scripts.upload_options_to_s3.convert_dbn_to_parquet",
        side_effect=_fake_convert,
    ):
        run_pipeline(config, dry_run=False, resume=False)

    # Check state
    state = PipelineState(config.state_file)
    state.load()
    assert state.summary()["completed"] == 3

    # Check S3
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
    keys = [obj["Key"] for obj in response.get("Contents", [])]
    assert len(keys) == 3

    # Verify Hive partitioning in keys
    assert any("underlying=SPY" in k for k in keys)
    assert any("date=2025-08-07" in k for k in keys)
    assert any("date=2025-08-08" in k for k in keys)
    assert any("date=2025-08-11" in k for k in keys)


@mock_aws
def test_resume_skips_completed(tiny_zip: Path, tmp_path: Path):
    """Resume skips files that were already completed."""
    from scripts.upload_options_to_s3 import run_pipeline

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    config = PipelineConfig(
        zip_path=tiny_zip,
        extract_dir=tmp_path / "extract",
        bucket_name=BUCKET,
        s3_prefix=PREFIX,
        state_file=tmp_path / "state.json",
    )

    # Run first time (processes all 3)
    with patch(
        "scripts.upload_options_to_s3.convert_dbn_to_parquet",
        side_effect=_fake_convert,
    ) as mock_convert:
        run_pipeline(config, dry_run=False, resume=False)
        first_run_calls = mock_convert.call_count

    assert first_run_calls == 3

    # Run again with resume (should process 0)
    with patch(
        "scripts.upload_options_to_s3.convert_dbn_to_parquet",
        side_effect=_fake_convert,
    ) as mock_convert:
        run_pipeline(config, dry_run=False, resume=True)
        second_run_calls = mock_convert.call_count

    assert second_run_calls == 0


@mock_aws
def test_no_temp_dirs_left_after_run(tiny_zip: Path, tmp_path: Path):
    """No .pipeline_ temp directories remain after a successful run."""
    from scripts.upload_options_to_s3 import run_pipeline

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)

    config = PipelineConfig(
        zip_path=tiny_zip,
        extract_dir=tmp_path / "extract",
        bucket_name=BUCKET,
        s3_prefix=PREFIX,
        state_file=tmp_path / "state.json",
    )

    with patch(
        "scripts.upload_options_to_s3.convert_dbn_to_parquet",
        side_effect=_fake_convert,
    ):
        run_pipeline(config, dry_run=False, resume=False)

    # Check no temp dirs left in the ZIP's parent directory
    zip_parent = tiny_zip.parent
    temp_dirs = [d for d in zip_parent.iterdir() if d.name.startswith(".pipeline_")]
    assert len(temp_dirs) == 0
