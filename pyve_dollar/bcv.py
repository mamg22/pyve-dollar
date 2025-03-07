import datetime
from urllib.parse import urljoin

from lxml import html
import requests
import xlrd

from .database import get_database
from .common import (
    eprint,
    REDENOMINATION_DAY,
    REDENOMINATION_FACTOR,
    VE_TZ,
    PLATFORM_DIRS,
)

SOURCE_NAME = "BCV"

STATS_URL = "https://www.bcv.org.ve/estadisticas/tipo-cambio-de-referencia-smc"
STATS_CACHE = PLATFORM_DIRS.user_cache_path / "stats"


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


def download_stats(urls: list[str]):
    for idx, url in enumerate(urls):
        filename = url.split("/")[-1]
        cache_path = STATS_CACHE / filename

        # First (newest) url is always fetched again to ensure up to date data
        # rest is only fetched if not available
        if idx > 0 and cache_path.exists():
            continue

        try:
            eprint(f"Fetching {filename} from {url}")
            response = requests.get(url, verify=False)
            response.raise_for_status()
        except requests.RequestException as err:
            eprint(f"Error fetching {url}:\n{err}")
            continue

        cache_path.write_bytes(response.content)


def build_database():
    STATS_CACHE.mkdir(exist_ok=True)
    try:
        urls = fetch_stats_urls()
        download_stats(urls)
    except requests.ConnectionError:
        eprint("Failed to fetch current stats index, data might be outdated")

    rates: list[tuple[datetime.date, int]] = []
    for file in STATS_CACHE.iterdir():
        try:
            book = xlrd.open_workbook(file, on_demand=True)

        except xlrd.XLRDError:
            eprint(f"Could not open file {file}")
            continue

        for sheet in book:
            date_cell = sheet[4][3]
            rate_date = datetime.datetime.strptime(
                date_cell.value.split()[-1], "%d/%m/%Y"
            ).astimezone(VE_TZ)

            # Values are stored with up to four decimal places convert to
            # integer to keep precision and avoid floating point rounding
            val = int(sheet[14][-1].value * 10000)

            # Adjust VES values into VED
            if rate_date < REDENOMINATION_DAY:
                val //= REDENOMINATION_FACTOR

            rates.append((rate_date, val))

    db = get_database()

    db.executemany(
        f"INSERT INTO rates(time, source, rate) VALUES (?, '{SOURCE_NAME}', ?) ON CONFLICT (time, source) DO NOTHING",
        rates,
    )
    db.commit()
