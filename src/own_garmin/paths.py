import os
from datetime import date

from dotenv import load_dotenv

load_dotenv()


def data_root() -> str:
    return os.environ.get("OWN_GARMIN_DATA_DIR", "./data")


def session_dir() -> str:
    path = os.environ.get("OWN_GARMIN_SESSION_DIR", "~/.config/own-garmin/session")
    return os.path.expanduser(path)


def bronze_path(category: str, day: date) -> str:
    root = data_root()
    return f"{root}/bronze/{category}/year={day:%Y}/month={day:%m}/day={day:%d}.json"


def silver_path(category: str) -> str:
    return f"{data_root()}/silver/{category}"
