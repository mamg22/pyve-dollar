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
CREATE TABLE IF NOT EXISTS rates (
    time DATETIME NOT NULL PRIMARY KEY,
    source TEXT NOT NULL,
    rate INTEGER NOT NULL,
    UNIQUE (time, source)
);
"""


def get_database(**connect_options) -> sqlite3.Connection:
    db = sqlite3.connect(DB_PATH, **connect_options)

    db.executescript(SCHEMA)

    return db
