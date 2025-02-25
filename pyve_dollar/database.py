import datetime
import sqlite3


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


def get_database() -> sqlite3.Connection:
    db = sqlite3.connect("rates.db")

    db.executescript(SCHEMA)

    return db
