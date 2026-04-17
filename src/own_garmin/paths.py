import os
from datetime import date


def data_root() -> str:
    path = os.environ.get("OWN_GARMIN_DATA_DIR", "./data")
    if path.startswith("s3://"):
        return path.rstrip("/")
    return os.path.expanduser(path)


def session_dir() -> str:
    path = os.environ.get("OWN_GARMIN_SESSION_DIR", "~/.config/own-garmin/session")
    return os.path.expanduser(path)


def bronze_path(category: str, day: date) -> str:
    root = data_root()
    return f"{root}/bronze/{category}/year={day:%Y}/month={day:%m}/day={day:%d}.json"


def bronze_fit_path(activity_id: int, day: date) -> str:
    root = data_root()
    return (
        f"{root}/bronze/fit/year={day:%Y}/month={day:%m}/day={day:%d}/{activity_id}.zip"
    )


def silver_path(category: str) -> str:
    return f"{data_root()}/silver/{category}"


def silver_glob(category: str) -> str:
    return f"{silver_path(category)}/**/*.parquet"
