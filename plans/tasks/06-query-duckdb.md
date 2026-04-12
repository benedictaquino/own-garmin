# Task 06: DuckDB query helper

## Goal

Provide a one-call SQL interface over the silver parquet files.

## File

`src/own_garmin/query.py`

## Public API

```python
import polars as pl

def query(sql: str) -> pl.DataFrame: ...
```

## Behavior

1. Open an in-memory DuckDB connection (`duckdb.connect()`).
2. Register a view per silver category so SQL can reference them by bare name:

   ```sql
   CREATE VIEW activities AS
   SELECT * FROM read_parquet('{silver_path("activities")}/**/*.parquet', hive_partitioning=1);
   ```

3. `result = con.execute(sql).pl()` — return Polars DataFrame.
4. Close the connection.

## Acceptance

- `query("SELECT COUNT(*) AS n FROM activities")` returns a 1-row DataFrame matching the parquet row count.
- `query("SELECT year, COUNT(*) FROM activities GROUP BY year")` works without an explicit `read_parquet` call in user SQL.
- Missing parquet (empty silver) raises a clear error, not a cryptic DuckDB message — wrap if needed.

## Notes

- Only one category (activities) exists in v1; register additional views in this module as new silver categories land.
- Do not persist the DuckDB database to disk — recreating views on each call is cheap and avoids stale-schema bugs.
- Keep this file small; it's a convenience wrapper, not a query layer.
