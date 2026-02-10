"""Configuration for the options-to-S3 pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """All configuration for the options upload pipeline."""

    # Source
    zip_path: Path
    extract_dir: Path

    # S3 target
    bucket_name: str
    s3_prefix: str = "options/cbbo-1m"
    aws_region: str = "us-east-1"

    # Processing
    underlying_symbol: str = "SPY"
    state_file: Path = field(default_factory=lambda: Path("data/staging/.upload_state.json"))

    # Tuning
    cleanup_after_upload: bool = True

    @classmethod
    def from_env(cls, zip_path: str | Path) -> PipelineConfig:
        """Build config from environment variables + defaults."""
        return cls(
            zip_path=Path(zip_path),
            extract_dir=Path(os.getenv("EXTRACT_DIR", "data/staging/extracted")),
            bucket_name=os.getenv("S3_BUCKET_NAME", ""),
            s3_prefix=os.getenv("S3_PREFIX", "options/cbbo-1m"),
            aws_region=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            underlying_symbol=os.getenv("UNDERLYING_SYMBOL", "SPY"),
            state_file=Path(os.getenv("STATE_FILE", "data/staging/.upload_state.json")),
            cleanup_after_upload=os.getenv("CLEANUP_AFTER_UPLOAD", "true").lower() == "true",
        )
