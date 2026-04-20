import json
import logging

import polars as pl

from own_garmin import paths, storage

_LOGGER = logging.getLogger(__name__)

# Mapping from Garmin metric key -> canonical column name
_METRIC_KEY_TO_COL: dict[str, str] = {
    "directTimestamp": "timestamp",
    "sumDuration": "duration_sec",
    "sumElapsedDuration": "elapsed_duration_sec",
    "sumMovingDuration": "moving_duration_sec",
    "sumDistance": "distance_m",
    "sumAccumulatedPower": "accumulated_power_w",
    "directHeartRate": "heart_rate",
    "directSpeed": "speed_mps",
    "directGradeAdjustedSpeed": "grade_adjusted_speed_mps",
    "directVerticalSpeed": "vertical_speed_mps",
    "directPower": "power_w",
    "directDoubleCadence": "cadence_spm",
    "directRunCadence": "run_cadence_spm",
    "directBikeCadence": "bike_cadence_rpm",
    "directFractionalCadence": "fractional_cadence",
    "directStrideLength": "stride_length_cm",
    "directVerticalOscillation": "vertical_oscillation_cm",
    "directVerticalRatio": "vertical_ratio",
    "directGroundContactTime": "ground_contact_time_ms",
    "directGroundContactBalanceLeft": "ground_contact_balance_left",
    "directRespirationRate": "respiration_rate_brpm",
    "directBodyBattery": "body_battery",
    "directAvailableStamina": "available_stamina",
    "directPotentialStamina": "potential_stamina",
    "directPerformanceCondition": "performance_condition",
    "directCaloriesBurnRate": "calories_burn_rate",
    "directAirTemperature": "air_temperature_c",
    "directElevation": "elevation_m",
    "directLatitude": "position_lat",
    "directLongitude": "position_lon",
}

# Schema used when building rows (timestamp stored as Int64 epoch-ms before casting)
_ROW_SCHEMA: dict[str, type] = {
    "activity_id": pl.Int64,
    "timestamp": pl.Int64,
    "duration_sec": pl.Float64,
    "elapsed_duration_sec": pl.Float64,
    "moving_duration_sec": pl.Float64,
    "distance_m": pl.Float64,
    "accumulated_power_w": pl.Float64,
    "heart_rate": pl.Float64,
    "speed_mps": pl.Float64,
    "grade_adjusted_speed_mps": pl.Float64,
    "vertical_speed_mps": pl.Float64,
    "power_w": pl.Float64,
    "cadence_spm": pl.Float64,
    "run_cadence_spm": pl.Float64,
    "bike_cadence_rpm": pl.Float64,
    "fractional_cadence": pl.Float64,
    "stride_length_cm": pl.Float64,
    "vertical_oscillation_cm": pl.Float64,
    "vertical_ratio": pl.Float64,
    "ground_contact_time_ms": pl.Float64,
    "ground_contact_balance_left": pl.Float64,
    "respiration_rate_brpm": pl.Float64,
    "body_battery": pl.Float64,
    "available_stamina": pl.Float64,
    "potential_stamina": pl.Float64,
    "performance_condition": pl.Float64,
    "calories_burn_rate": pl.Float64,
    "air_temperature_c": pl.Float64,
    "elevation_m": pl.Float64,
    "position_lat": pl.Float64,
    "position_lon": pl.Float64,
}

# Final output schema (timestamp as Datetime, with year/month partition cols)
_OUTPUT_SCHEMA: dict[str, type] = {
    "activity_id": pl.Int64,
    "timestamp": pl.Datetime("ms"),
    "duration_sec": pl.Float64,
    "elapsed_duration_sec": pl.Float64,
    "moving_duration_sec": pl.Float64,
    "distance_m": pl.Float64,
    "accumulated_power_w": pl.Float64,
    "heart_rate": pl.Float64,
    "speed_mps": pl.Float64,
    "grade_adjusted_speed_mps": pl.Float64,
    "vertical_speed_mps": pl.Float64,
    "power_w": pl.Float64,
    "cadence_spm": pl.Float64,
    "run_cadence_spm": pl.Float64,
    "bike_cadence_rpm": pl.Float64,
    "fractional_cadence": pl.Float64,
    "stride_length_cm": pl.Float64,
    "vertical_oscillation_cm": pl.Float64,
    "vertical_ratio": pl.Float64,
    "ground_contact_time_ms": pl.Float64,
    "ground_contact_balance_left": pl.Float64,
    "respiration_rate_brpm": pl.Float64,
    "body_battery": pl.Float64,
    "available_stamina": pl.Float64,
    "potential_stamina": pl.Float64,
    "performance_condition": pl.Float64,
    "calories_burn_rate": pl.Float64,
    "air_temperature_c": pl.Float64,
    "elevation_m": pl.Float64,
    "position_lat": pl.Float64,
    "position_lon": pl.Float64,
    "year": pl.Int32,
    "month": pl.Int32,
}


def _empty_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=_OUTPUT_SCHEMA)


def transform(bronze_json_paths: list[str]) -> pl.DataFrame:
    """Transform activity_details JSON into per-(activity_id, timestamp) rows."""
    if not bronze_json_paths:
        return _empty_frame()

    rows: list[dict] = []
    for path in bronze_json_paths:
        raw = json.loads(storage.read_bytes(path).decode())
        for item in raw:
            activity_id = item["activityId"]
            descriptors = item.get("metricDescriptors", [])
            detail_metrics = item.get("activityDetailMetrics", [])

            # Build index -> canonical column name map
            idx_to_col: dict[int, str] = {}
            logged_unknown: set[str] = set()
            for d in descriptors:
                key = d["key"]
                col = _METRIC_KEY_TO_COL.get(key)
                if col is None:
                    if key not in logged_unknown:
                        _LOGGER.debug(
                            "activity %s: unknown metric key %r — skipping",
                            activity_id,
                            key,
                        )
                        logged_unknown.add(key)
                else:
                    idx_to_col[d["metricsIndex"]] = col

            for metric_row in detail_metrics:
                values = metric_row["metrics"]
                row: dict = {"activity_id": activity_id}
                for idx, col in idx_to_col.items():
                    if idx < len(values):
                        v = values[idx]
                        # Store timestamp as int for schema compatibility
                        if col == "timestamp" and v is not None:
                            row[col] = int(v)
                        else:
                            row[col] = v
                    else:
                        row[col] = None
                rows.append(row)

    if not rows:
        return _empty_frame()

    df = pl.DataFrame(rows, schema=_ROW_SCHEMA)

    # Cast epoch-ms int to Datetime("ms")
    df = df.with_columns(
        pl.from_epoch(pl.col("timestamp"), time_unit="ms")
        .cast(pl.Datetime("ms"))
        .alias("timestamp"),
    )

    # Derive partition columns
    df = df.with_columns(
        pl.col("timestamp").dt.year().cast(pl.Int32).alias("year"),
        pl.col("timestamp").dt.month().cast(pl.Int32).alias("month"),
    )

    # Enforce column order per spec
    df = df.select(list(_OUTPUT_SCHEMA.keys()))

    # Deduplicate on (activity_id, timestamp), keep last
    return df.unique(
        subset=["activity_id", "timestamp"], keep="last", maintain_order=True
    )


def rebuild() -> int:
    """Rebuild silver from all bronze/activity_details/**/*.json. Returns row count."""
    pattern = f"{paths.data_root()}/bronze/activity_details/**/*.json"
    files = storage.list_files(pattern)
    df = transform(files)

    target = paths.silver_path("activity_metrics")
    storage.rmtree(target)
    if df.height == 0:
        return 0

    df = df.with_columns(
        pl.col("year").cast(pl.Utf8),
        pl.col("month").cast(pl.Utf8).str.zfill(2),
    )

    storage.write_partitioned_parquet(df, target, ["year", "month"])
    return df.height
