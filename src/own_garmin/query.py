import glob

import duckdb
import polars as pl

from own_garmin import paths


def query(sql: str) -> pl.DataFrame:
    """Execute SQL against silver parquet views and return a Polars DataFrame."""
    silver_activities = paths.silver_path("activities")
    pattern = f"{silver_activities}/**/*.parquet"
    files = glob.glob(pattern, recursive=True)
    if not files:
        raise FileNotFoundError(
            f"No parquet files found under '{silver_activities}'. "
            "Run `own-garmin process` to build the silver layer first."
        )

    con = duckdb.connect()
    try:
        parquet_glob = f"{silver_activities}/**/*.parquet"
        con.execute(
            f"CREATE VIEW activities AS "
            f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1)"
        )
        return con.execute(sql).pl()
    finally:
        con.close()
