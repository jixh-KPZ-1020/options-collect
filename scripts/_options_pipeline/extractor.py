"""ZIP extraction utilities for Databento data files.

Extracts individual .dbn.zst files one at a time to minimize disk usage.
Verifies file hashes against the manifest when available.
"""

from __future__ import annotations

import hashlib
import json
import re
import zipfile
from datetime import date
from pathlib import Path


def list_dbn_files(zip_path: Path) -> list[str]:
    """List all .dbn.zst filenames in the ZIP, sorted by date."""
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.endswith(".dbn.zst")]
    return sorted(names)


def parse_date_from_filename(filename: str) -> date:
    """Extract date from 'opra-pillar-YYYYMMDD.cbbo-1m.dbn.zst'.

    Raises ValueError if the filename doesn't contain a valid date.
    """
    match = re.search(r"(\d{8})", filename)
    if not match:
        raise ValueError(f"No date found in filename: {filename}")

    date_str = match.group(1)
    return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))


def extract_dbn_file(zip_path: Path, member_name: str, dest_path: Path) -> Path:
    """Extract a single .dbn.zst file from the ZIP archive.

    Args:
        zip_path: Path to the ZIP file.
        member_name: Name of the member to extract (e.g. 'opra-pillar-20250807.cbbo-1m.dbn.zst').
        dest_path: Full path where the extracted file should be written.

    Returns:
        Path to the extracted file.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(member_name) as src, open(dest_path, "wb") as dst:
            while True:
                chunk = src.read(8 * 1024 * 1024)  # 8 MB chunks
                if not chunk:
                    break
                dst.write(chunk)

    return dest_path


def load_manifest_hashes(zip_path: Path) -> dict[str, str]:
    """Load SHA-256 hashes from manifest.json inside the ZIP.

    Returns a dict mapping filename -> expected SHA-256 hex digest.
    Returns empty dict if manifest.json is not found.
    """
    try:
        with zipfile.ZipFile(zip_path) as zf:
            if "manifest.json" not in zf.namelist():
                return {}
            manifest = json.loads(zf.read("manifest.json"))
    except (zipfile.BadZipFile, json.JSONDecodeError, KeyError):
        return {}

    hashes: dict[str, str] = {}
    for file_info in manifest.get("files", []):
        filename = file_info.get("filename", "")
        hash_str = file_info.get("hash", "")
        if hash_str.startswith("sha256:"):
            hashes[filename] = hash_str[7:]  # Strip "sha256:" prefix
    return hashes


def verify_file_hash(file_path: Path, expected_sha256: str) -> None:
    """Verify a file's SHA-256 matches the expected value.

    Raises ValueError on mismatch.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            sha256.update(chunk)

    actual = sha256.hexdigest()
    if actual != expected_sha256:
        raise ValueError(
            f"Hash mismatch for {file_path.name}: "
            f"expected {expected_sha256[:16]}..., got {actual[:16]}..."
        )
