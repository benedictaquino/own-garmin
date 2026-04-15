import functools
import shutil
from datetime import date
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

load_dotenv()

app = typer.Typer(no_args_is_help=True)


def _handle_errors(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            typer.echo(f"error: {e}", err=True)
            raise typer.Exit(code=1)

    return wrapper


@app.command()
@_handle_errors
def login() -> None:
    """Force a fresh Garmin login and persist new session tokens."""
    from own_garmin import paths
    from own_garmin.client import GarminClient

    session = Path(paths.session_dir())
    if session.exists():
        shutil.rmtree(session)
    client = GarminClient()
    typer.echo(f"session: {client.session_dir}")


@app.command()
@_handle_errors
def ingest(
    since: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    until: Optional[str] = typer.Option(
        None, help="End date (YYYY-MM-DD); defaults to today"
    ),
    sleep_sec: float = typer.Option(0.5, help="Sleep between detail/FIT requests"),
) -> None:
    """Ingest activity summaries, details, and FIT files from Garmin into bronze."""
    from own_garmin.bronze import activities, activity_details, fit
    from own_garmin.client import GarminClient

    since_date = date.fromisoformat(since)
    until_date = date.fromisoformat(until) if until else date.today()

    client = GarminClient()
    activity_list = client.list_activities(since_date, until_date)

    n_activities = activities.ingest(activity_list)
    typer.echo(f"Activities: {n_activities} written")

    n_details = activity_details.ingest(client, activity_list, sleep_sec=sleep_sec)
    typer.echo(f"Activity details: {n_details} day-files written")

    n_fit = fit.ingest(client, activity_list, sleep_sec=sleep_sec)
    typer.echo(f"FIT files: {n_fit} written")


@app.command()
@_handle_errors
def process() -> None:
    """Rebuild the silver activities parquet from bronze JSON."""
    from own_garmin.silver import activities

    n = activities.rebuild()
    typer.echo(f"Silver activities: {n} rows written")


@app.command()
@_handle_errors
def query(
    sql: str = typer.Argument(
        ..., help="SQL query to run against silver parquet views"
    ),
) -> None:
    """Run a SQL query against the silver parquet layer and print the result."""
    from own_garmin.query import query as run_query

    df = run_query(sql)
    typer.echo(df)
