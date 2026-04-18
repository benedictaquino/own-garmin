import os
from urllib.parse import urlparse

import duckdb
import polars as pl

from own_garmin import paths, storage

_SILVER_TABLES = ("activities", "fit_records")


def query(sql: str) -> pl.DataFrame:
    """Execute SQL against silver parquet views and return a Polars DataFrame."""
    con = duckdb.connect()
    try:
        if storage.is_s3(paths.data_root()):
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            con.install_extension("aws")
            con.load_extension("aws")
            con.execute("CALL load_aws_credentials();")
            endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
            if endpoint:
                parsed = urlparse(endpoint)
                host = parsed.netloc or parsed.path
                use_ssl = parsed.scheme.lower() == "https"
                con.execute(f"SET s3_endpoint='{host}';")
                con.execute("SET s3_url_style='path';")
                con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
            con.execute(f"SET temp_directory='{paths.duckdb_temp_dir()}';")

        registered: list[str] = []
        for name in _SILVER_TABLES:
            parquet_glob = paths.silver_glob(name)
            if not storage.list_files(parquet_glob):
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
