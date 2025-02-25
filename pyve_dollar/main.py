import argparse
import sqlite3
import sys

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
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
