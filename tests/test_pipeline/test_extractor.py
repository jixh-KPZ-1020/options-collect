"""Tests for ZIP extraction and hash verification."""

from __future__ import annotations

import hashlib
import json
import sys
import zipfile
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.extractor import (
    extract_dbn_file,
    list_dbn_files,
    load_manifest_hashes,
    parse_date_from_filename,
    verify_file_hash,
)


@pytest.fixture
def sample_zip(tmp_path: Path) -> Path:
    """Create a ZIP with fake .dbn.zst files and manifest."""
    zip_path = tmp_path / "test.zip"
    files = {
        "opra-pillar-20250807.cbbo-1m.dbn.zst": b"data for day 7",
        "opra-pillar-20250808.cbbo-1m.dbn.zst": b"data for day 8",
        "opra-pillar-20250811.cbbo-1m.dbn.zst": b"data for day 11",
        "metadata.json": b'{"job_id": "TEST"}',
    }

    manifest_files = []
    for name, content in files.items():
        if name.endswith(".dbn.zst"):
            sha = hashlib.sha256(content).hexdigest()
            manifest_files.append({
                "filename": name,
                "size": len(content),
                "hash": f"sha256:{sha}",
            })

    manifest = json.dumps({"job_id": "TEST", "files": manifest_files})

    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
        zf.writestr("manifest.json", manifest)

    return zip_path


class TestListDbnFiles:
    def test_lists_only_dbn_zst(self, sample_zip: Path):
        files = list_dbn_files(sample_zip)
        assert len(files) == 3
        assert all(f.endswith(".dbn.zst") for f in files)

    def test_sorted_by_date(self, sample_zip: Path):
        files = list_dbn_files(sample_zip)
        assert files[0] < files[1] < files[2]


class TestParseDateFromFilename:
    def test_standard_filename(self):
        d = parse_date_from_filename("opra-pillar-20250807.cbbo-1m.dbn.zst")
        assert d == date(2025, 8, 7)

    def test_no_date_raises(self):
        with pytest.raises(ValueError, match="No date found"):
            parse_date_from_filename("nodate.dbn.zst")


class TestExtractDbnFile:
    def test_extracts_correct_content(self, sample_zip: Path, tmp_path: Path):
        dest = tmp_path / "extracted" / "test.dbn.zst"
        result = extract_dbn_file(
            sample_zip, "opra-pillar-20250807.cbbo-1m.dbn.zst", dest
        )
        assert result == dest
        assert dest.read_bytes() == b"data for day 7"

    def test_creates_parent_dirs(self, sample_zip: Path, tmp_path: Path):
        dest = tmp_path / "deep" / "nested" / "file.dbn.zst"
        extract_dbn_file(sample_zip, "opra-pillar-20250807.cbbo-1m.dbn.zst", dest)
        assert dest.exists()


class TestManifestHashes:
    def test_loads_sha256_hashes(self, sample_zip: Path):
        hashes = load_manifest_hashes(sample_zip)
        assert len(hashes) == 3
        for name, h in hashes.items():
            assert name.endswith(".dbn.zst")
            assert len(h) == 64  # SHA-256 hex

    def test_no_manifest_returns_empty(self, tmp_path: Path):
        zip_path = tmp_path / "no_manifest.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("test.dbn.zst", b"data")
        assert load_manifest_hashes(zip_path) == {}


class TestVerifyFileHash:
    def test_matching_hash_passes(self, tmp_path: Path):
        content = b"test content"
        file_path = tmp_path / "test.file"
        file_path.write_bytes(content)

        expected = hashlib.sha256(content).hexdigest()
        verify_file_hash(file_path, expected)  # Should not raise

    def test_mismatched_hash_raises(self, tmp_path: Path):
        file_path = tmp_path / "test.file"
        file_path.write_bytes(b"actual content")

        with pytest.raises(ValueError, match="Hash mismatch"):
            verify_file_hash(file_path, "0" * 64)
