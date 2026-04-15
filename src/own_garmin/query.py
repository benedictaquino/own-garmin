import glob

import duckdb
import polars as pl

from own_garmin import paths

_SILVER_TABLES = ("activities", "fit_records")


def query(sql: str) -> pl.DataFrame:
    """Execute SQL against silver parquet views and return a Polars DataFrame."""
    con = duckdb.connect()
    try:
        registered: list[str] = []
        for name in _SILVER_TABLES:
            parquet_glob = paths.silver_glob(name)
            if not glob.glob(parquet_glob, recursive=True):
                continue
            con.read_parquet(parquet_glob, hive_partitioning=True).create_view(name)
            registered.append(name)

        if not registered:
            raise FileNotFoundError(
                f"No silver parquet found under '{paths.data_root()}/silver'. "
                "Run `own-garmin process` to build the silver layer first."
            )

        return con.execute(sql).pl()
    finally:
        con.close()
