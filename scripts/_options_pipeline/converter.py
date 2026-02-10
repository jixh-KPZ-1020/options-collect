"""Convert Databento .dbn.zst files to Parquet.

Uses databento.DBNStore.to_parquet() for streaming conversion that
avoids loading the entire DataFrame into memory.
"""

from __future__ import annotations

from pathlib import Path


def convert_dbn_to_parquet(dbn_path: Path, parquet_path: Path) -> Path:
    """Convert a .dbn.zst file to Parquet.

    Uses databento's streaming to_parquet() to avoid full DataFrame
    materialization. Each daily file can decompress to 500MB+ as a
    DataFrame, so streaming is critical.

    Args:
        dbn_path: Path to the .dbn.zst file.
        parquet_path: Path where the Parquet file should be written.

    Returns:
        Path to the written Parquet file.
    """
    import databento as db

    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    store = db.DBNStore.from_file(str(dbn_path))
    store.to_parquet(
        str(parquet_path),
        pretty_ts=True,       # Convert nanosecond timestamps to readable datetimes
        map_symbols=False,    # Keep instrument_id (skip loading 178MB symbology)
        price_type="float",   # Convert fixed-point prices to float64
    )

    return parquet_path
