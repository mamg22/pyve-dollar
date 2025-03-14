import asyncio
import datetime
import re
import os

from telethon import TelegramClient

from .common import (
    eprint,
    PLATFORM_DIRS,
    REDENOMINATION_DAY,
    REDENOMINATION_FACTOR,
    VE_TZ,
)
from .database import get_database

type RateData = tuple[datetime.datetime, int]

SOURCE_NAME = "paralelo"

SESSION_FILE = PLATFORM_DIRS.user_data_path / "paralelo.session"

DATA_REGEX = re.compile(
    r"""(\d{1,2}/\d{1,2}/\d{2,4})       # Match date in [d]d/[m]m/[yy]yy format
        [^BS/]*?                        # Avoid hitting a currency symbol
        ((?:10|11|12|\d)[:\.;]*\d{2})\b # Match time in [h]h:MM format
        [^B]*?                          # Keep going until hitting a currency symbol
        Bs.\ ([0-9.,]+\d)               # Match the value, which contains numbers and separators
    """,
    flags=(re.VERBOSE + re.DOTALL + re.IGNORECASE),
)


def parse_date(date_str: str) -> datetime.date:
    year, month, day = map(int, reversed(date_str.split("/")))

    if year < 2000:
        year += 2000

    return datetime.date(year, month, day)


def parse_time(time_str: str) -> datetime.time:
    hour, minutes = int(time_str[:-2].strip(":;.")), int(time_str[-2:])

    if hour < 7:
        hour += 12

    return datetime.time(int(hour), int(minutes))


def parse_value(value_str: str) -> int:
    decimals_str = value_str[-3:]

    if decimals_str[0] in ".,":
        decimals = int(value_str[-2:])
        whole = int(value_str[:-3].replace(".", ""))

        # Convert to 1/10000s of a cent
        return whole * 10000 + decimals * 100
    else:
        return int(value_str[:-3].replace(".", "")) * 10000


def parse_message(message: str) -> RateData | None:
    message = re.sub(r"[^0-9BbSs/,\.:;% ]+", " ", message)
    if match := DATA_REGEX.search(message):
        date_str, time_str, value_str = match.groups()
        date = parse_date(date_str)
        time = parse_time(time_str)
        value = parse_value(value_str)

        rate_date = datetime.datetime.combine(date, time, VE_TZ)

        if rate_date < REDENOMINATION_DAY:
            value //= REDENOMINATION_FACTOR

        return rate_date, value
    else:
        return None


def fix_quirks(data: RateData) -> RateData:
    """Apply corrections to certain wrong values in the source data"""
    date, value = data

    # Mistyped value
    if date.date() == datetime.date(2024, 5, 29) and value == 411_1000:
        value = 41_1000
    # Wrong year and value
    elif (
        date == datetime.datetime(2024, 1, 3, 12, 45, tzinfo=VE_TZ) and value == 6_0800
    ):
        date = date.replace(year=2025)
        value = 67_0800
    # Wrong year 2024 -> 2025
    elif (
        datetime.date(2024, 1, 6) <= date.date() <= datetime.date(2024, 1, 8)
        and 64_0000 <= value <= 70_0000
    ):
        date = date.replace(year=2025)
    # Wrong year 2022 -> 2024
    elif (
        datetime.date(2022, 1, 5) <= date.date() <= datetime.date(2022, 1, 9)
        and 20_0000 <= value <= 22_0000
    ):
        date = date.replace(year=2023)
    # Missing digit in value
    elif date == datetime.datetime(2021, 2, 16, 9, tzinfo=VE_TZ) and value == 1733:
        value = 17330
    elif date == datetime.datetime(2020, 7, 23, 13, tzinfo=VE_TZ) and value == 261:
        value = 2610
    # Parser failure?
    elif date.date() == datetime.date(2020, 3, 13) and value == 0:
        value = 775

    return (date, value)


async def fetch(
    last_fetched_id: int,
) -> tuple[list[RateData], int | None]:
    try:
        api_id = int(os.environ["PYVE_DOLLAR_TG_ID"])
        api_hash = os.environ["PYVE_DOLLAR_TG_HASH"]
    except KeyError as err:
        eprint(f"Required credential `{err.args[0]}` not provided")
        raise
    except ValueError:
        eprint("Telegram api ID is not a valid number")
        raise

    client = TelegramClient(SESSION_FILE, api_id, api_hash)

    async with client:
        ichannel = await client.get_input_entity("enparalelovzlatelegram")
        channel = await client.get_entity(ichannel)

        if isinstance(channel, list):
            channel = channel[0]

        msgs = client.iter_messages(
            channel,
            search="Bs.",
            reverse=True,
            wait_time=2,
            offset_id=last_fetched_id,
        )

        texts: list[str] = []
        msg = None
        async for msg in msgs:
            texts.append(msg.message)

        last_msg_id: int | None
        if msg is not None:
            last_msg_id = msg.id
        else:
            last_msg_id = None

    eprint(f"Fetched {len(texts)} messages")

    rates: list[RateData] = []
    for msg in texts:
        data = parse_message(msg)
        if data is not None:
            data = fix_quirks(data)
            rates.append(data)
        else:
            eprint(f"Unable to parse message `{msg[:100].replace("\n", "")}`")

    return rates, last_msg_id


def build_database():
    db = get_database()

    last_fetched_id = db.execute(
        f"SELECT value FROM RatesMeta WHERE source = '{SOURCE_NAME}' AND key = 'last_fetched_id'"
    )

    id_record = last_fetched_id.fetchone()

    last_id = id_record[0] if id_record is not None else 0

    rates, new_last_id = asyncio.run(fetch(last_id))

    db.executemany(
        f"INSERT INTO Rates(time, source, rate) VALUES (?, '{SOURCE_NAME}', ?) ON CONFLICT (time, source) DO NOTHING",
        rates,
    )
    db.executemany(
        f"INSERT INTO RatesMeta(source, key, value) VALUES ('{SOURCE_NAME}', ?, ?) ON CONFLICT (source, key) DO UPDATE SET value=value",
        (
            ("last_update", datetime.datetime.now()),
            ("last_fetched_id", new_last_id if new_last_id is not None else last_id),
        ),
    )

    db.commit()
