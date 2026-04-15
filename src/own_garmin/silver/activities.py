import glob
import shutil
from pathlib import Path

import polars as pl

from own_garmin import paths

SEMICIRCLE_TO_DEG = 180.0 / 2**31

_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

_SCHEMA = {
    "activity_id": pl.Int64,
    "activity_type": pl.Utf8,
    "start_time_local": pl.Datetime,
    "start_time_utc": pl.Datetime,
    "duration_sec": pl.Float64,
    "distance_m": pl.Float64,
    "avg_hr": pl.Float64,
    "max_hr": pl.Float64,
    "calories": pl.Float64,
    "elevation_gain_m": pl.Float64,
    "elevation_loss_m": pl.Float64,
    "start_lat": pl.Float64,
    "start_lon": pl.Float64,
    "year": pl.Int32,
    "month": pl.Int32,
}


def transform(bronze_json_paths: list[str]) -> pl.DataFrame:
    """Transform activity summaries from bronze JSON into a typed DataFrame."""
    frames = [pl.read_json(p) for p in bronze_json_paths]
    if not frames:
        return _empty_frame()

    raw = pl.concat(frames, how="diagonal_relaxed")
    if raw.height == 0:
        return _empty_frame()

    columns = set(raw.columns)

    def nullable(name: str, dtype) -> pl.Expr:
        if name in columns:
            return pl.col(name).cast(dtype)
        return pl.lit(None, dtype=dtype)

    if "activityType" in columns:
        activity_type = pl.col("activityType").struct.field("typeKey").cast(pl.Utf8)
    else:
        activity_type = pl.lit(None, dtype=pl.Utf8)

    def parse_dt(name: str) -> pl.Expr:
        if name in columns:
            return pl.col(name).str.strptime(
                pl.Datetime, _DATETIME_FORMAT, strict=False
            )
        return pl.lit(None, dtype=pl.Datetime)

    df = raw.select(
        pl.col("activityId").cast(pl.Int64).alias("activity_id"),
        activity_type.alias("activity_type"),
        parse_dt("startTimeLocal").alias("start_time_local"),
        parse_dt("startTimeGMT").alias("start_time_utc"),
        nullable("duration", pl.Float64).alias("duration_sec"),
        nullable("distance", pl.Float64).alias("distance_m"),
        nullable("averageHR", pl.Float64).alias("avg_hr"),
        nullable("maxHR", pl.Float64).alias("max_hr"),
        nullable("calories", pl.Float64).alias("calories"),
        nullable("elevationGain", pl.Float64).alias("elevation_gain_m"),
        nullable("elevationLoss", pl.Float64).alias("elevation_loss_m"),
        (nullable("startLatitude", pl.Float64) * SEMICIRCLE_TO_DEG).alias("start_lat"),
        (nullable("startLongitude", pl.Float64) * SEMICIRCLE_TO_DEG).alias("start_lon"),
    ).with_columns(
        pl.col("start_time_local").dt.year().cast(pl.Int32).alias("year"),
        pl.col("start_time_local").dt.month().cast(pl.Int32).alias("month"),
    )

    return df.unique(subset=["activity_id"], keep="last")


def rebuild() -> int:
    """Rebuild activities silver from bronze JSON. Returns row count written."""
    pattern = f"{paths.data_root()}/bronze/activities/**/*.json"
    files = sorted(glob.glob(pattern, recursive=True))
    df = transform(files)

    target = paths.silver_path("activities")
    shutil.rmtree(target, ignore_errors=True)
    if df.height == 0:
        return 0

    Path(target).mkdir(parents=True, exist_ok=True)
    df.write_parquet(target, partition_by=["year", "month"])
    return df.height


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_SCHEMA)
