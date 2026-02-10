"""Post-run validation: independently verify the complete S3 dataset.

Cross-references three sources of truth:
1. S3 inventory (list all objects)
2. Pipeline state file (completed entries)
3. ZIP manifest (expected files)
"""

from __future__ import annotations

import json
import random
import re
import zipfile
from pathlib import Path

import boto3

from _options_pipeline.state import PipelineState


def validate_s3_dataset(
    bucket: str,
    s3_prefix: str,
    state_path: Path,
    zip_path: Path,
    region: str = "us-east-1",
    expected_count: int | None = None,
) -> dict:
    """Validate the complete dataset on S3 after a pipeline run.

    Returns a report dict with per-check pass/fail status.
    """
    s3 = boto3.client("s3", region_name=region)
    report: dict = {"checks": [], "overall": "pass"}

    # --- Check 1: S3 inventory ---
    s3_objects = _list_all_objects(s3, bucket, s3_prefix)
    # Filter to only .parquet files (exclude .preflight_test etc)
    parquet_objects = {k: v for k, v in s3_objects.items() if k.endswith(".parquet")}

    if expected_count is not None:
        count_ok = len(parquet_objects) == expected_count
    else:
        count_ok = len(parquet_objects) > 0

    report["checks"].append({
        "name": "s3_object_count",
        "expected": expected_count or "at least 1",
        "actual": len(parquet_objects),
        "status": "pass" if count_ok else "fail",
    })

    # --- Check 2: State file cross-reference ---
    state = PipelineState(state_path)
    state_data = state.load()
    state_files = state_data.get("files", {})

    completed_entries = {
        k: v for k, v in state_files.items() if v.get("status") == "completed"
    }

    # Verify each completed entry has a matching S3 object with correct size
    missing_from_s3 = []
    size_mismatches = []
    for filename, info in completed_entries.items():
        s3_key = info.get("s3_key", "")
        if s3_key not in s3_objects:
            missing_from_s3.append(s3_key)
        elif s3_objects[s3_key]["size"] != info.get("size_bytes"):
            size_mismatches.append({
                "key": s3_key,
                "state_size": info.get("size_bytes"),
                "s3_size": s3_objects[s3_key]["size"],
            })

    report["checks"].append({
        "name": "state_s3_consistency",
        "completed_in_state": len(completed_entries),
        "missing_from_s3": missing_from_s3,
        "size_mismatches": size_mismatches,
        "status": "pass" if not missing_from_s3 and not size_mismatches else "fail",
    })

    # --- Check 3: Manifest completeness ---
    expected_dates = _dates_from_manifest(zip_path)
    actual_dates = set()
    for key in parquet_objects:
        match = re.search(r"date=(\d{4}-\d{2}-\d{2})", key)
        if match:
            actual_dates.add(match.group(1))

    missing_dates = expected_dates - actual_dates
    report["checks"].append({
        "name": "date_completeness",
        "expected_dates": len(expected_dates),
        "actual_dates": len(actual_dates),
        "missing": sorted(missing_dates),
        "status": "pass" if not missing_dates else "fail",
    })

    # --- Check 4: No zero-byte objects ---
    zero_byte = [k for k, v in parquet_objects.items() if v["size"] == 0]
    report["checks"].append({
        "name": "no_zero_byte_objects",
        "zero_byte_count": len(zero_byte),
        "status": "pass" if not zero_byte else "fail",
    })

    # --- Check 5: Parquet spot-check (magic bytes) ---
    sample_keys = random.sample(
        list(parquet_objects.keys()),
        min(3, len(parquet_objects)),
    )
    spot_results = []
    for key in sample_keys:
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            # Read just the first and last 4 bytes to check PAR1 magic
            data = resp["Body"].read()
            is_parquet = data[:4] == b"PAR1" and data[-4:] == b"PAR1"
            spot_results.append({
                "key": key,
                "size": len(data),
                "valid_parquet": is_parquet,
                "status": "pass" if is_parquet else "fail",
            })
        except Exception as e:
            spot_results.append({"key": key, "error": str(e), "status": "fail"})

    report["checks"].append({
        "name": "parquet_spot_check",
        "samples": spot_results,
        "status": "pass" if all(r["status"] == "pass" for r in spot_results) else "fail",
    })

    # --- Overall ---
    if any(c["status"] == "fail" for c in report["checks"]):
        report["overall"] = "fail"

    return report


def print_validation_report(report: dict) -> None:
    """Print a human-readable validation report."""
    overall = report["overall"].upper()
    marker = "PASS" if overall == "PASS" else "FAIL"
    print(f"\n{'='*50}")
    print(f"Validation Result: {marker}")
    print(f"{'='*50}\n")

    for check in report["checks"]:
        status = check["status"].upper()
        icon = "OK" if status == "PASS" else "FAIL"
        print(f"  [{icon}] {check['name']}")

        for key, val in check.items():
            if key in ("name", "status"):
                continue
            if isinstance(val, list) and len(val) == 0:
                continue
            print(f"       {key}: {val}")

    print()


def _list_all_objects(s3_client, bucket: str, prefix: str) -> dict[str, dict]:
    """List all S3 objects under a prefix, handling pagination."""
    objects = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = {
                "size": obj["Size"],
                "etag": obj["ETag"],
            }
    return objects


def _dates_from_manifest(zip_path: Path) -> set[str]:
    """Extract expected dates from the ZIP manifest."""
    dates: set[str] = set()
    try:
        with zipfile.ZipFile(zip_path) as zf:
            if "manifest.json" not in zf.namelist():
                return dates
            manifest = json.loads(zf.read("manifest.json"))
    except (zipfile.BadZipFile, json.JSONDecodeError):
        return dates

    for file_info in manifest.get("files", []):
        filename = file_info.get("filename", "")
        if not filename.endswith(".dbn.zst"):
            continue
        match = re.search(r"(\d{8})", filename)
        if match:
            d = match.group(1)
            dates.add(f"{d[:4]}-{d[4:6]}-{d[6:8]}")

    return dates
