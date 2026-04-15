from datetime import date

import typer
from dotenv import load_dotenv

load_dotenv()

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(ctx: typer.Context) -> None:
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())


@app.command()
def ingest(
    since: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    until: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
    sleep_sec: float = typer.Option(0.5, help="Sleep between detail/FIT requests"),
) -> None:
    """Ingest activity summaries, details, and FIT files from Garmin into bronze."""
    from own_garmin.bronze import activities, activity_details, fit
    from own_garmin.client import GarminClient

    since_date = date.fromisoformat(since)
    until_date = date.fromisoformat(until)

    client = GarminClient()
    activity_list = client.list_activities(since_date, until_date)

    n_activities = activities.ingest(activity_list)
    typer.echo(f"Activities: {n_activities} written")

    n_details = activity_details.ingest(client, activity_list, sleep_sec=sleep_sec)
    typer.echo(f"Activity details: {n_details} day-files written")

    n_fit = fit.ingest(client, activity_list, sleep_sec=sleep_sec)
    typer.echo(f"FIT files: {n_fit} written")


@app.command()
def query(
    sql: str = typer.Argument(
        ..., help="SQL query to run against silver parquet views"
    ),
) -> None:
    """Run a SQL query against the silver parquet layer and print the result."""
    from own_garmin.query import query as run_query

    df = run_query(sql)
    typer.echo(df)
