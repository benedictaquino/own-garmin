import glob
import logging
import zipfile
from pathlib import Path

import polars as pl
from garmin_fit_sdk import Decoder, Stream

from own_garmin import paths

_LOGGER = logging.getLogger(__name__)

SEMICIRCLE_TO_DEG = 180.0 / 2**31

_RECORD_FIELDS = (
    "timestamp",
    "heart_rate",
    "cadence",
    "speed",
    "power",
    "distance",
    "altitude",
    "position_lat",
    "position_long",
)

_ROW_SCHEMA = {
    "activity_id": pl.Int64,
    "timestamp": pl.Datetime,
    "heart_rate": pl.Int32,
    "cadence": pl.Int32,
    "speed": pl.Float64,
    "power": pl.Int32,
    "distance": pl.Float64,
    "altitude": pl.Float64,
    "position_lat": pl.Float64,
    "position_long": pl.Float64,
}

_OUTPUT_SCHEMA = {
    "activity_id": pl.Int64,
    "timestamp": pl.Datetime,
    "heart_rate": pl.Int32,
    "cadence": pl.Int32,
    "speed": pl.Float64,
    "power": pl.Int32,
    "distance": pl.Float64,
    "altitude": pl.Float64,
    "position_lat": pl.Float64,
    "position_lon": pl.Float64,
    "year": pl.Int32,
    "month": pl.Int32,
}


def transform(fit_zip_paths: list[str]) -> pl.DataFrame:
    """Transform FIT ZIPs into per-second telemetry rows."""
    frames: list[pl.DataFrame] = []
    for zip_path in fit_zip_paths:
        df = _decode_zip(zip_path)
        if df is not None and df.height > 0:
            frames.append(df)

    if not frames:
        return pl.DataFrame(schema=_OUTPUT_SCHEMA)

    raw = pl.concat(frames, how="diagonal_relaxed")
    df = raw.with_columns(
        (pl.col("position_lat").cast(pl.Float64) * SEMICIRCLE_TO_DEG).alias(
            "position_lat"
        ),
        (pl.col("position_long").cast(pl.Float64) * SEMICIRCLE_TO_DEG).alias(
            "position_lon"
        ),
    ).with_columns(
        pl.col("timestamp").dt.year().cast(pl.Int32).alias("year"),
        pl.col("timestamp").dt.month().cast(pl.Int32).alias("month"),
    )

    df = df.select(
        "activity_id",
        "timestamp",
        "heart_rate",
        "cadence",
        "speed",
        "power",
        "distance",
        "altitude",
        "position_lat",
        "position_lon",
        "year",
        "month",
    )

    return df.unique(subset=["activity_id", "timestamp"], keep="last")


def rebuild() -> int:
    """Rebuild fit_records silver from bronze FIT ZIPs. Returns row count."""
    pattern = f"{paths.data_root()}/bronze/fit/**/*.zip"
    files = sorted(glob.glob(pattern, recursive=True))
    df = transform(files)
    if df.height == 0:
        return 0

    target = paths.silver_path("fit_records")
    Path(target).mkdir(parents=True, exist_ok=True)
    df.write_parquet(target, partition_by=["year", "month"])
    return df.height


def _decode_zip(zip_path: str) -> pl.DataFrame | None:
    try:
        activity_id = int(Path(zip_path).stem)
    except ValueError:
        _LOGGER.warning("FIT ZIP %s has non-integer stem, skipping", zip_path)
        return None

    try:
        with zipfile.ZipFile(zip_path) as zf:
            fit_names = [n for n in zf.namelist() if n.endswith(".fit")]
            if not fit_names:
                _LOGGER.warning("FIT ZIP %s contains no .fit file, skipping", zip_path)
                return None
            fit_bytes = zf.read(fit_names[0])
    except (zipfile.BadZipFile, OSError) as exc:
        _LOGGER.warning("FIT ZIP %s unreadable (%s), skipping", zip_path, exc)
        return None

    try:
        messages, errors = Decoder(Stream.from_byte_array(fit_bytes)).read()
    except Exception as exc:
        _LOGGER.warning("FIT decode failed for %s (%s), skipping", zip_path, exc)
        return None

    if errors:
        _LOGGER.warning("FIT decode errors for %s: %s, skipping", zip_path, errors)
        return None

    records = messages.get("record_mesgs", [])
    rows = [
        {"activity_id": activity_id, **{f: rec.get(f) for f in _RECORD_FIELDS}}
        for rec in records
    ]
    if not rows:
        return None

    return pl.DataFrame(rows, schema=_ROW_SCHEMA)
