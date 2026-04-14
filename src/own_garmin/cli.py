import datetime

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

    since_date = datetime.date.fromisoformat(since)
    until_date = datetime.date.fromisoformat(until)

    client = GarminClient()

    n_activities = activities.ingest(client, since_date, until_date)
    typer.echo(f"Activities: {n_activities} written")

    n_details = activity_details.ingest(
        client, since_date, until_date, sleep_sec=sleep_sec
    )
    typer.echo(f"Activity details: {n_details} day-files written")

    n_fit = fit.ingest(client, since_date, until_date, sleep_sec=sleep_sec)
    typer.echo(f"FIT files: {n_fit} written")
