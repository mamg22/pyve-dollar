import argparse
import sqlite3

from pyve_dollar.common import VE_TZ
from pyve_dollar.database import get_database

from . import bcv
from . import paralelo


def show_plot():
    import matplotlib.pyplot as plt

    db = sqlite3.connect("rates.db", detect_types=sqlite3.PARSE_DECLTYPES)
    rates_BCV = db.execute(
        "SELECT time, cast(rate as float) / 10000 FROM rates WHERE source = 'BCV' ORDER BY datetime(time)"
    ).fetchall()
    rates_paralelo = db.execute(
        "SELECT time, cast(rate as float) / 10000 FROM rates WHERE source = 'paralelo' ORDER BY datetime(time)"
    ).fetchall()

    _, ax = plt.subplots()

    ax.set_ylabel("Valor Bs/$ (VED/USD)")
    ax.set_xlabel("Fecha")

    ax.plot(*zip(*rates_BCV))
    ax.plot(*zip(*rates_paralelo))

    plt.show()


def interactive():
    import datetime

    while True:
        date_str = input("Date: ")
        try:
            date = datetime.datetime.fromisoformat(date_str).astimezone(VE_TZ)
            break
        except ValueError:
            print("Invalid date")

    db = get_database()

    rates_now = db.execute(
        """
        WITH RecencyRates AS (
            SELECT time, source, rate, row_number() OVER (PARTITION BY source ORDER BY time DESC) AS rn
            FROM rates
            WHERE datetime(time) <= datetime(?)
        )
        SELECT time, source, rate
        FROM RecencyRates
        WHERE rn = 1
        """,
        (date.isoformat(),),
    ).fetchall()

    while True:
        try:
            val = float(input("$ "))
        except ValueError:
            print("Invalid number")
            continue

        for time, source, rate in rates_now:
            rate /= 10000
            time = datetime.datetime.fromisoformat(time)

            print(
                f"Value for ${val} based on {source} at {time} ({rate}):\tBs. {rate * val:.02f}"
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-b",
        "--build-database",
        action="store_true",
        help="Build `rates.db` database file",
    )
    parser.add_argument("-p", "--show-plot", action="store_true", help="Show plot")

    args = parser.parse_args()

    if args.build_database:
        bcv.build_database()
        paralelo.build_database()
    elif args.show_plot:
        show_plot()
    else:
        interactive()


if __name__ == "__main__":
    main()
