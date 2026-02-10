"""Atomic state file management for resumable pipeline runs.

Uses write-to-temp-then-rename to guarantee the state file is never
partially written, even on crash or Ctrl+C.
"""

from __future__ import annotations

import datetime
import json
import os
import tempfile
from pathlib import Path
from typing import Any


class PipelineState:
    """Manages pipeline state with atomic file writes."""

    def __init__(self, state_path: Path) -> None:
        self._path = state_path
        self._data: dict[str, Any] = {}

    @property
    def data(self) -> dict[str, Any]:
        return self._data

    def load(self) -> dict[str, Any]:
        """Load state from disk. Returns empty dict if file doesn't exist."""
        if self._path.exists():
            self._data = json.loads(self._path.read_text())
        else:
            self._data = {}
        return self._data

    def save(self) -> None:
        """Atomically write state to disk.

        Writes to a temporary file in the same directory, then uses
        os.replace() for an atomic rename. This guarantees the state
        file is never partially written.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(self._data, indent=2, default=str)

        # Create temp file in the SAME directory as state file
        # (required for atomic os.replace on same filesystem)
        fd, tmp_path = tempfile.mkstemp(
            dir=self._path.parent,
            prefix=".state_",
            suffix=".tmp",
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
            os.replace(tmp_path, self._path)  # Atomic rename
        except BaseException:
            # Clean up temp file on any failure (including KeyboardInterrupt)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def init_job(
        self,
        job_id: str,
        zip_path: str,
        bucket: str,
        prefix: str,
    ) -> None:
        """Initialize state for a new pipeline run."""
        if not self._data:
            self._data = {
                "job_id": job_id,
                "zip_path": zip_path,
                "s3_bucket": bucket,
                "s3_prefix": prefix,
                "started_at": _now_iso(),
                "last_updated": _now_iso(),
                "files": {},
            }
            self.save()

    def mark_completed(self, filename: str, **metadata: Any) -> None:
        """Mark a file as completed and atomically persist."""
        self._data.setdefault("files", {})[filename] = {
            "status": "completed",
            "completed_at": _now_iso(),
            **metadata,
        }
        self._data["last_updated"] = _now_iso()
        self.save()

    def mark_failed(self, filename: str, error: str) -> None:
        """Mark a file as failed and atomically persist."""
        self._data.setdefault("files", {})[filename] = {
            "status": "failed",
            "error": error,
            "failed_at": _now_iso(),
        }
        self._data["last_updated"] = _now_iso()
        self.save()

    def pending_files(self, all_files: list[str]) -> list[str]:
        """Return files that are not yet completed (includes failed for retry)."""
        files = self._data.get("files", {})
        return [f for f in all_files if files.get(f, {}).get("status") != "completed"]

    def summary(self) -> dict[str, int]:
        """Return counts of completed, failed, and pending files."""
        files = self._data.get("files", {})
        completed = sum(1 for f in files.values() if f.get("status") == "completed")
        failed = sum(1 for f in files.values() if f.get("status") == "failed")
        return {"completed": completed, "failed": failed}


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
