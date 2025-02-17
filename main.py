import argparse
import datetime
import pathlib
import sqlite3
from urllib.parse import urljoin

from lxml import html
import matplotlib.pyplot as plt
import requests
import xlrd


STATS_URL = "https://www.bcv.org.ve/estadisticas/tipo-cambio-de-referencia-smc"


def fetch_stats_urls() -> list[str]:
    next_url = STATS_URL

    download_links = []
    while next_url:
        # TODO: Find out why it can't check the certificate
        response = requests.get(str(next_url), verify=False)
        response.raise_for_status()
        document = html.fromstring(response.content)
        main_block = document.get_element_by_id("block-system-main")

        download_links.extend(
            el.attrib["href"]
            for el in map(
                lambda icon: icon.getparent(), main_block.find_class("file-icon")
            )
            if el is not None
        )

        pagination = main_block.find_class("pagination")[0]

        if next := pagination.find_class("next"):
            anchor = next[0][0]
            next_url = urljoin(next_url, anchor.attrib["href"])
        else:
            break

    return download_links


STATS_CACHE = pathlib.Path("cache")


def download_stats(urls: list[str]):
    STATS_CACHE.mkdir(exist_ok=True)

    for idx, url in enumerate(urls):
        filename = url.split("/")[-1]
        cache_path = STATS_CACHE / filename

        # First (newest) url is always fetched again to ensure up to date data
        # rest is only fetched if not available
        if idx > 0 and cache_path.exists():
            continue

        try:
            print(f"Fetching {filename} from {url}")
            response = requests.get(url, verify=False)
            response.raise_for_status()
        except requests.RequestException as err:
            print(f"Error fetching {url}:\n{err}")
            continue

        cache_path.write_bytes(response.content)


def adapt_date(val: datetime.date) -> str:
    return val.isoformat()


def convert_date(val: bytes) -> datetime.date:
    return datetime.date.fromisoformat(val.decode())


sqlite3.register_adapter(datetime.date, adapt_date)
sqlite3.register_converter("date", convert_date)


SCHEMA = """\
CREATE TABLE IF NOT EXISTS rates (
    day DATE PRIMARY KEY,
    rate INTEGER NOT NULL
);
"""


def build_database():
    try:
        urls = fetch_stats_urls()
        download_stats(urls)
    except requests.ConnectionError:
        print("Failed to fetch current stats index, data might be outdated")

    rates: list[tuple[datetime.date, int]] = []
    for file in STATS_CACHE.iterdir():
        try:
            book = xlrd.open_workbook(file, on_demand=True)
        except xlrd.XLRDError:
            print(f"Could not open file {file}")
            continue

        for sheet in book:
            date_cell = sheet[4][3]
            rate_date = datetime.datetime.strptime(
                date_cell.value.split()[-1], "%d/%m/%Y"
            ).date()

            # Values are stored with up to four decimal places convert to
            # integer to keep precision and avoid floating point rounding
            val = int(sheet[14][-1].value * 10000)

            # Adjust VES values into VED
            if rate_date < datetime.date(2021, 10, 1):
                val //= 1_000_000

            rates.append((rate_date, val))

    db = sqlite3.connect("rates.db")
    db.executescript(SCHEMA)

    db.executemany(
        "INSERT INTO rates VALUES (?, ?) ON CONFLICT (day) DO NOTHING", rates
    )
    db.commit()


def show_plot():
    db = sqlite3.connect("rates.db", detect_types=sqlite3.PARSE_DECLTYPES)
    res = db.execute(
        "SELECT day, cast(rate as float) / 10000 FROM rates ORDER BY day"
    ).fetchall()

    fig, ax = plt.subplots()

    ax.set_ylabel("Valor Bs/$ (VED/USD)")
    ax.set_xlabel("Fecha")

    ax.plot(*zip(*res))

    plt.show()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--build-database", action="store_true")

    args = parser.parse_args()

    if args.build_database:
        build_database()
    else:
        show_plot()


if __name__ == "__main__":
    main()
