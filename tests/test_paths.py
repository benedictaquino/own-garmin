import os
from datetime import date

import pytest

import own_garmin.paths as paths


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    monkeypatch.delenv("OWN_GARMIN_DATA_DIR", raising=False)
    monkeypatch.delenv("OWN_GARMIN_SESSION_DIR", raising=False)


def test_data_root_default():
    assert paths.data_root() == "./data"


def test_data_root_from_env(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", "/tmp/foo")
    assert paths.data_root() == "/tmp/foo"


def test_session_dir_default():
    expected = os.path.expanduser("~/.config/own-garmin/session")
    assert paths.session_dir() == expected


def test_session_dir_from_env(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_SESSION_DIR", "/tmp/sessions")
    assert paths.session_dir() == "/tmp/sessions"


def test_session_dir_expanduser(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_SESSION_DIR", "~/my-sessions")
    assert paths.session_dir() == os.path.expanduser("~/my-sessions")


def test_bronze_path(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", "/tmp/foo")
    result = paths.bronze_path("activities", date(2026, 4, 12))
    assert result == "/tmp/foo/bronze/activities/year=2026/month=04/day=12.json"


def test_bronze_path_default_root():
    result = paths.bronze_path("activities", date(2026, 1, 5))
    assert result == "./data/bronze/activities/year=2026/month=01/day=05.json"


def test_silver_path(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", "/tmp/foo")
    result = paths.silver_path("activities")
    assert result == "/tmp/foo/silver/activities"


def test_silver_path_default_root():
    result = paths.silver_path("activities")
    assert result == "./data/silver/activities"


def test_paths_return_strings(monkeypatch):
    monkeypatch.setenv("OWN_GARMIN_DATA_DIR", "/tmp/foo")
    assert isinstance(paths.data_root(), str)
    assert isinstance(paths.session_dir(), str)
    assert isinstance(paths.bronze_path("activities", date(2026, 4, 12)), str)
    assert isinstance(paths.silver_path("activities"), str)
