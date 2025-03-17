import datetime
from typing import Literal

from fastapi import FastAPI

from pyve_dollar.database import get_database

app = FastAPI()


@app.get("/")
async def root(
    source: Literal["BCV", "paralelo"],
    value: int,
    date: datetime.datetime | None = None,
):
    db = get_database()

    if date is None:
        date = datetime.datetime.now()

    result = db.execute(
        """
        SELECT time, source, rate
        FROM Rates
        WHERE source = :source
            AND datetime(time) <= datetime(:date)
        ORDER BY datetime(time) DESC
        LIMIT 1
        """,
        {"source": source, "date": date},
    ).fetchone()

    if result is not None:
        _, source, rate = result

        return rate * value // 10000
    else:
        return None
