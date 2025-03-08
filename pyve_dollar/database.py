import datetime
import sqlite3

from .common import PLATFORM_DIRS

DB_PATH = PLATFORM_DIRS.user_data_path / "rates.db"


def adapt_datetime(val: datetime.datetime) -> str:
    return val.isoformat()


def convert_datetime(val: bytes) -> datetime.datetime:
    return datetime.datetime.fromisoformat(val.decode())


sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)


SCHEMA = """\
CREATE TABLE IF NOT EXISTS Rates (
    time DATETIME NOT NULL,
    source TEXT NOT NULL,
    rate INTEGER NOT NULL,
    PRIMARY KEY (time, source)
);
CREATE TABLE IF NOT EXISTS RatesMeta (
    source TEXT NOT NULL,
    key TEXT NOT NULL,
    value ANY,
    PRIMARY KEY (source, key)
);
"""


def get_database(**connect_options) -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, **connect_options)

    db.executescript(SCHEMA)

    return db
