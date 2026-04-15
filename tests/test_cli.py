from datetime import date
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from own_garmin.cli import app

runner = CliRunner()


def test_help_lists_all_commands():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for cmd in ("login", "ingest", "process", "query"):
        assert cmd in result.output


def test_process_calls_rebuild_and_prints_count():
    with patch("own_garmin.silver.activities.rebuild", return_value=42) as mock_rebuild:
        result = runner.invoke(app, ["process"])
    assert result.exit_code == 0
    mock_rebuild.assert_called_once()
    assert "42" in result.output


def test_process_error_exits_nonzero():
    with patch(
        "own_garmin.silver.activities.rebuild", side_effect=RuntimeError("boom")
    ):
        result = runner.invoke(app, ["process"])
    assert result.exit_code == 1
    assert "error: boom" in result.output


def test_login_wipes_session_and_prints_dir(tmp_path):
    fake_session = tmp_path / "session"
    fake_session.mkdir()
    (fake_session / "garmin_tokens.json").write_text("{}")

    mock_client = MagicMock()
    mock_client.session_dir = fake_session

    with (
        patch("own_garmin.paths.session_dir", return_value=str(fake_session)),
        patch("own_garmin.client.GarminClient", return_value=mock_client),
    ):
        result = runner.invoke(app, ["login"])

    assert result.exit_code == 0
    assert str(fake_session) in result.output
    # token file was wiped before client construction
    assert not (fake_session / "garmin_tokens.json").exists()


def test_login_error_exits_nonzero(tmp_path):
    fake_session = tmp_path / "session"
    with (
        patch("own_garmin.paths.session_dir", return_value=str(fake_session)),
        patch(
            "own_garmin.client.GarminClient", side_effect=RuntimeError("auth failed")
        ),
    ):
        result = runner.invoke(app, ["login"])
    assert result.exit_code == 1
    assert "error: auth failed" in result.output


def test_ingest_until_defaults_to_today():
    mock_client = MagicMock()
    mock_client.list_activities.return_value = []

    with (
        patch("own_garmin.client.GarminClient", return_value=mock_client),
        patch("own_garmin.bronze.activities.ingest", return_value=0),
        patch("own_garmin.bronze.activity_details.ingest", return_value=0),
        patch("own_garmin.bronze.fit.ingest", return_value=0),
    ):
        result = runner.invoke(app, ["ingest", "--since", "2026-01-01"])

    assert result.exit_code == 0
    since_arg, until_arg = mock_client.list_activities.call_args[0]
    assert since_arg == date(2026, 1, 1)
    assert until_arg == date.today()


def test_query_prints_result():
    import polars as pl

    df = pl.DataFrame({"x": [1]})
    with patch("own_garmin.query.query", return_value=df):
        result = runner.invoke(app, ["query", "SELECT 1 AS x"])
    assert result.exit_code == 0
    assert "x" in result.output


def test_query_error_exits_nonzero():
    with patch("own_garmin.query.query", side_effect=Exception("bad sql")):
        result = runner.invoke(app, ["query", "SELECT broken"])
    assert result.exit_code == 1
    assert "error: bad sql" in result.output
