"""S3 upload with checksum verification and retry logic.

Three layers of verification:
1. Content-MD5 header on upload — S3 rejects if mismatch
2. Post-upload HeadObject — verify ContentLength matches
3. SHA-256 recorded in state file — enables post-run audit
"""

from __future__ import annotations

import base64
import hashlib
import time
from datetime import date
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, EndpointConnectionError

MAX_RETRIES = 3
MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB

_transfer_config = TransferConfig(
    multipart_threshold=MULTIPART_THRESHOLD,
    multipart_chunksize=64 * 1024 * 1024,  # 64 MB chunks
    max_concurrency=4,
    use_threads=True,
)


def build_s3_key(
    prefix: str,
    underlying: str,
    trade_date: date,
    filename: str = "data.parquet",
) -> str:
    """Build S3 key with Hive partitioning.

    Returns e.g. 'options/cbbo-1m/underlying=SPY/date=2025-08-07/data.parquet'
    """
    return f"{prefix}/underlying={underlying}/date={trade_date.isoformat()}/{filename}"


def compute_file_hashes(file_path: Path) -> tuple[str, str]:
    """Compute SHA-256 and MD5 of a file in a single pass.

    Returns (sha256_hex, md5_base64).
    """
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            sha256.update(chunk)
            md5.update(chunk)

    return sha256.hexdigest(), base64.b64encode(md5.digest()).decode()


def upload_to_s3(
    local_path: Path,
    bucket: str,
    s3_key: str,
    region: str = "us-east-1",
    s3_client: boto3.client | None = None,
) -> dict[str, str | int]:
    """Upload a file to S3 with integrity verification and retry.

    For files under 100MB, uses single PutObject with Content-MD5.
    For larger files, uses multipart upload via transfer manager.

    Returns dict with 'etag', 'size_bytes', 'sha256'.
    """
    if s3_client is None:
        s3_client = boto3.client("s3", region_name=region)

    file_size = local_path.stat().st_size
    sha256_hex, md5_b64 = compute_file_hashes(local_path)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if file_size < MULTIPART_THRESHOLD:
                # Single PUT with Content-MD5 verification
                with open(local_path, "rb") as f:
                    response = s3_client.put_object(
                        Bucket=bucket,
                        Key=s3_key,
                        Body=f,
                        ContentMD5=md5_b64,
                    )
                etag = response.get("ETag", "")
            else:
                # Multipart upload for large files
                callback = _UploadProgress(local_path.name, file_size)
                s3_client.upload_file(
                    str(local_path),
                    bucket,
                    s3_key,
                    Config=_transfer_config,
                    Callback=callback,
                )
                print()  # Newline after progress
                head = s3_client.head_object(Bucket=bucket, Key=s3_key)
                etag = head.get("ETag", "")

            # Post-upload verification
            verify_s3_upload(s3_client, bucket, s3_key, file_size)

            return {
                "etag": etag,
                "size_bytes": file_size,
                "sha256": sha256_hex,
            }

        except (ClientError, EndpointConnectionError, OSError) as e:
            if attempt == MAX_RETRIES:
                raise
            wait = 2**attempt
            print(f"  Retry {attempt}/{MAX_RETRIES} in {wait}s: {e}")
            time.sleep(wait)

    # Should not reach here, but satisfy type checker
    raise RuntimeError("Upload failed after all retries")


def verify_s3_upload(
    s3_client: boto3.client,
    bucket: str,
    key: str,
    expected_size: int,
) -> bool:
    """Verify an S3 object exists with the expected size.

    Raises ValueError on size mismatch, ClientError on 404.
    """
    response = s3_client.head_object(Bucket=bucket, Key=key)
    actual_size = response["ContentLength"]

    if actual_size != expected_size:
        raise ValueError(
            f"S3 size mismatch for {key}: expected {expected_size}, got {actual_size}"
        )
    return True


def abort_stale_multipart_uploads(
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
    max_age_hours: int = 24,
) -> int:
    """Abort incomplete multipart uploads older than max_age_hours."""
    import datetime

    aborted = 0
    now = datetime.datetime.now(datetime.timezone.utc)

    try:
        response = s3_client.list_multipart_uploads(Bucket=bucket, Prefix=prefix)
    except ClientError:
        return 0

    for upload in response.get("Uploads", []):
        age = now - upload["Initiated"]
        if age.total_seconds() > max_age_hours * 3600:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=upload["Key"],
                    UploadId=upload["UploadId"],
                )
                aborted += 1
            except ClientError:
                pass

    return aborted


class _UploadProgress:
    """Progress callback for boto3 multipart uploads."""

    def __init__(self, filename: str, filesize: int) -> None:
        self._filename = filename
        self._size = filesize
        self._seen = 0

    def __call__(self, bytes_amount: int) -> None:
        self._seen += bytes_amount
        pct = (self._seen / self._size) * 100
        print(f"\r  Uploading {self._filename}: {pct:.1f}%", end="", flush=True)
