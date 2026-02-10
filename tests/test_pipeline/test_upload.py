"""Tests for S3 upload and verification logic."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.uploader import (
    build_s3_key,
    compute_file_hashes,
    upload_to_s3,
    verify_s3_upload,
)


class TestBuildS3Key:
    """Test Hive-partitioned S3 key construction."""

    def test_basic_key(self):
        from datetime import date

        key = build_s3_key("options/cbbo-1m", "SPY", date(2025, 8, 7))
        assert key == "options/cbbo-1m/underlying=SPY/date=2025-08-07/data.parquet"

    def test_custom_filename(self):
        from datetime import date

        key = build_s3_key("prefix", "QQQ", date(2025, 1, 15), filename="chunk.parquet")
        assert key == "prefix/underlying=QQQ/date=2025-01-15/chunk.parquet"


class TestComputeFileHashes:
    """Test SHA-256 and MD5 computation."""

    def test_known_content(self, tmp_path: Path):
        content = b"known content for hash verification"
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(content)

        sha256_hex, md5_b64 = compute_file_hashes(test_file)

        assert sha256_hex == hashlib.sha256(content).hexdigest()
        assert len(sha256_hex) == 64

        import base64

        expected_md5 = base64.b64encode(hashlib.md5(content).digest()).decode()
        assert md5_b64 == expected_md5

    def test_empty_file(self, tmp_path: Path):
        test_file = tmp_path / "empty.parquet"
        test_file.write_bytes(b"")

        sha256_hex, md5_b64 = compute_file_hashes(test_file)
        assert sha256_hex == hashlib.sha256(b"").hexdigest()


class TestUploadToS3:
    """Test upload logic with mocked S3 client."""

    def test_small_file_uses_put_object(self, tmp_path: Path):
        test_file = tmp_path / "small.parquet"
        test_file.write_bytes(b"small parquet data")

        mock_client = MagicMock()
        mock_client.put_object.return_value = {"ETag": '"abc123"'}
        mock_client.head_object.return_value = {
            "ContentLength": len(b"small parquet data"),
            "ETag": '"abc123"',
        }

        result = upload_to_s3(
            test_file, "bucket", "key.parquet", s3_client=mock_client
        )

        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args.kwargs
        assert "ContentMD5" in call_kwargs
        assert result["etag"] == '"abc123"'
        assert result["size_bytes"] == len(b"small parquet data")
        assert len(result["sha256"]) == 64


class TestVerifyS3Upload:
    """Test post-upload verification."""

    def test_size_match_passes(self):
        mock_client = MagicMock()
        mock_client.head_object.return_value = {"ContentLength": 12345}

        assert verify_s3_upload(mock_client, "bucket", "key", 12345) is True

    def test_size_mismatch_raises(self):
        mock_client = MagicMock()
        mock_client.head_object.return_value = {"ContentLength": 99999}

        with pytest.raises(ValueError, match="size mismatch"):
            verify_s3_upload(mock_client, "bucket", "key", 12345)

    def test_not_found_propagates(self):
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}},
            "HeadObject",
        )

        with pytest.raises(ClientError):
            verify_s3_upload(mock_client, "bucket", "key", 12345)
