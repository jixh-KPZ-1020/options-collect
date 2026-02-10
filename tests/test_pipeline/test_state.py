"""Tests for atomic state file management."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from _options_pipeline.state import PipelineState


class TestPipelineStateSave:
    """Test atomic write behavior."""

    def test_save_creates_file(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {"job_id": "test"}
        state.save()

        assert state_path.exists()
        assert json.loads(state_path.read_text())["job_id"] == "test"

    def test_save_no_temp_files_left(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {"key": "value"}
        state.save()

        files = list(tmp_path.iterdir())
        assert len(files) == 1
        assert files[0].name == "state.json"

    def test_save_preserves_original_on_write_failure(self, tmp_path: Path):
        state_path = tmp_path / "state.json"

        # Write initial state
        state = PipelineState(state_path)
        state._data = {"version": 1}
        state.save()

        # Simulate failure during write
        state._data = {"version": 2}
        with patch("os.fdopen", side_effect=OSError("disk full")):
            with pytest.raises(OSError, match="disk full"):
                state.save()

        # Original state should be intact
        assert json.loads(state_path.read_text())["version"] == 1

    def test_save_cleans_temp_on_failure(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {"version": 1}
        state.save()

        state._data = {"version": 2}
        with patch("os.fsync", side_effect=OSError("I/O error")):
            with pytest.raises(OSError):
                state.save()

        # No leftover temp files
        temp_files = [f for f in tmp_path.iterdir() if f.name.startswith(".state_")]
        assert len(temp_files) == 0

    def test_rapid_successive_saves(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)

        for i in range(50):
            state._data = {"iteration": i}
            state.save()

        final = json.loads(state_path.read_text())
        assert final["iteration"] == 49

    def test_save_creates_parent_directories(self, tmp_path: Path):
        state_path = tmp_path / "nested" / "deep" / "state.json"
        state = PipelineState(state_path)
        state._data = {"test": True}
        state.save()

        assert state_path.exists()


class TestPipelineStateLoad:
    """Test state loading."""

    def test_load_nonexistent_returns_empty(self, tmp_path: Path):
        state_path = tmp_path / "nonexistent.json"
        state = PipelineState(state_path)
        result = state.load()
        assert result == {}

    def test_load_existing_file(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps({"job_id": "test-123"}))

        state = PipelineState(state_path)
        result = state.load()
        assert result["job_id"] == "test-123"


class TestPipelineStateTracking:
    """Test mark_completed, mark_failed, pending_files."""

    def test_mark_completed_persists(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {"files": {}}

        state.mark_completed("file1.dbn.zst", s3_key="key1")

        on_disk = json.loads(state_path.read_text())
        assert on_disk["files"]["file1.dbn.zst"]["status"] == "completed"
        assert on_disk["files"]["file1.dbn.zst"]["s3_key"] == "key1"

    def test_mark_failed_persists(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {"files": {}}

        state.mark_failed("file1.dbn.zst", "connection timeout")

        on_disk = json.loads(state_path.read_text())
        assert on_disk["files"]["file1.dbn.zst"]["status"] == "failed"
        assert "connection timeout" in on_disk["files"]["file1.dbn.zst"]["error"]

    def test_pending_files_skips_completed(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {
            "files": {
                "file1.dbn.zst": {"status": "completed"},
                "file2.dbn.zst": {"status": "failed"},
            }
        }

        all_files = ["file1.dbn.zst", "file2.dbn.zst", "file3.dbn.zst"]
        pending = state.pending_files(all_files)

        # file1 completed -> skip. file2 failed -> retry. file3 new -> process.
        assert pending == ["file2.dbn.zst", "file3.dbn.zst"]

    def test_pending_files_empty_state(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {}

        all_files = ["a.dbn.zst", "b.dbn.zst"]
        assert state.pending_files(all_files) == all_files

    def test_summary_counts(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state = PipelineState(state_path)
        state._data = {
            "files": {
                "f1": {"status": "completed"},
                "f2": {"status": "completed"},
                "f3": {"status": "failed"},
            }
        }

        summary = state.summary()
        assert summary == {"completed": 2, "failed": 1}
