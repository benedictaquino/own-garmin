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

            fields = ["TYPE s3", "PROVIDER credential_chain"]
            endpoint = os.environ.get("AWS_ENDPOINT_URL_S3")
            if endpoint:
                parsed = urlparse(endpoint)
                host = parsed.hostname or parsed.path
                if parsed.port:
                    host = f"{host}:{parsed.port}"
                use_ssl = parsed.scheme.lower() == "https"
                region = os.environ.get("AWS_REGION", "us-east-1")
                safe_host = host.replace("'", "''")
                safe_region = region.replace("'", "''")
                fields.extend(
                    [
                        f"ENDPOINT '{safe_host}'",
                        "URL_STYLE 'path'",
                        f"USE_SSL {'true' if use_ssl else 'false'}",
                        f"REGION '{safe_region}'",
                    ]
                )
            secret_fields = ", ".join(fields)
            con.execute(f"CREATE OR REPLACE SECRET own_garmin_s3 ({secret_fields});")
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
