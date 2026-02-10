"""Tests for pre-flight validation checks."""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.config import PipelineConfig
from _options_pipeline.preflight import (
    PreflightError,
    cleanup_stale_temp_dirs,
    run_preflight_checks,
)


@pytest.fixture
def tiny_zip(tmp_path: Path) -> Path:
    """Create a minimal valid ZIP with .dbn.zst files."""
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("opra-pillar-20250807.cbbo-1m.dbn.zst", b"fake data")
        zf.writestr("opra-pillar-20250808.cbbo-1m.dbn.zst", b"fake data")
        zf.writestr("metadata.json", json.dumps({"job_id": "TEST"}))
    return zip_path


def _make_config(tmp_path: Path, zip_path: Path, **overrides) -> PipelineConfig:
    return PipelineConfig(
        zip_path=zip_path,
        extract_dir=tmp_path / "extract",
        bucket_name=overrides.get("bucket_name", "test-bucket"),
        state_file=tmp_path / "state.json",
        **{k: v for k, v in overrides.items() if k != "bucket_name"},
    )


class TestPreflightChecks:
    """Test pre-flight validation."""

    def test_missing_zip_fails(self, tmp_path: Path):
        config = _make_config(tmp_path, tmp_path / "nonexistent.zip")
        with pytest.raises(PreflightError, match="ZIP file not found"):
            run_preflight_checks(config, dry_run=True)

    def test_valid_zip_passes_in_dry_run(self, tmp_path: Path, tiny_zip: Path):
        config = _make_config(tmp_path, tiny_zip)
        results = run_preflight_checks(config, dry_run=True)

        assert results["zip_ok"] is True
        assert results["total_dbn_files"] == 2

    def test_corrupt_state_file_fails(self, tmp_path: Path, tiny_zip: Path):
        config = _make_config(tmp_path, tiny_zip)
        config.state_file.parent.mkdir(parents=True, exist_ok=True)
        config.state_file.write_text("{invalid json")

        with pytest.raises(PreflightError, match="State file corrupt"):
            run_preflight_checks(config, dry_run=True)

    def test_low_disk_space_fails(self, tmp_path: Path, tiny_zip: Path):
        config = _make_config(tmp_path, tiny_zip)
        # Simulate only 100 MB free (must be a named tuple with .free attribute)
        from collections import namedtuple
        DiskUsage = namedtuple("usage", ["total", "used", "free"])
        mock_usage = DiskUsage(100_000_000_000, 99_900_000_000, 100_000_000)
        with patch("shutil.disk_usage", return_value=mock_usage):
            with pytest.raises(PreflightError, match="Insufficient disk space"):
                run_preflight_checks(config, dry_run=True)

    def test_missing_databento_fails(self, tmp_path: Path, tiny_zip: Path):
        config = _make_config(tmp_path, tiny_zip)
        with patch.dict(sys.modules, {"databento": None}):
            # Force ImportError on import databento
            with patch("builtins.__import__", side_effect=_mock_import_no_databento):
                with pytest.raises(PreflightError, match="databento"):
                    run_preflight_checks(config, dry_run=True)


class TestCleanupStaleTempDirs:
    """Test stale temp directory cleanup."""

    def test_cleans_pipeline_prefixed_dirs(self, tmp_path: Path):
        stale1 = tmp_path / ".pipeline_20250807_abc"
        stale1.mkdir()
        (stale1 / "partial.parquet").write_bytes(b"data")

        stale2 = tmp_path / ".pipeline_20250808_def"
        stale2.mkdir()

        normal = tmp_path / "important_data"
        normal.mkdir()

        cleaned = cleanup_stale_temp_dirs(tmp_path)

        assert cleaned == 2
        assert not stale1.exists()
        assert not stale2.exists()
        assert normal.exists()

    def test_nonexistent_dir_returns_zero(self, tmp_path: Path):
        assert cleanup_stale_temp_dirs(tmp_path / "nope") == 0


def _mock_import_no_databento(name, *args, **kwargs):
    """Mock __import__ that blocks databento."""
    if name == "databento":
        raise ImportError("No module named 'databento'")
    return original_import(name, *args, **kwargs)


original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__
