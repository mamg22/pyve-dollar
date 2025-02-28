import asyncio
import datetime
import re
import os

from telethon import TelegramClient

from .common import eprint, REDENOMINATION_DAY, REDENOMINATION_FACTOR, VE_TZ
from .database import get_database

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


def parse_message(message: str) -> tuple[datetime.datetime, int] | None:
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


async def fetch():
    try:
        api_id = int(os.environ["PYVE_DOLLAR_TG_ID"])
        api_hash = os.environ["PYVE_DOLLAR_TG_HASH"]
    except KeyError as err:
        eprint(f"Required credential `{err.args[0]}` not provided")
        raise
    except ValueError:
        eprint("Telegram api ID is not a valid number")
        raise

    client = TelegramClient("paralelo", api_id, api_hash)

    async with client:
        ichannel = await client.get_input_entity("enparalelovzlatelegram")
        channel = await client.get_entity(ichannel)

        if isinstance(channel, list):
            channel = channel[0]

        msgs = client.iter_messages(channel, search="Bs.", reverse=False, wait_time=2)
        texts = []
        async for msg in msgs:
            texts.append(msg.message)

    rates = []
    for msg in texts:
        data = parse_message(msg)
        if data is not None:
            rates.append(data)
        else:
            eprint(f"Unable to parse message `{msg[:100].replace("\n", "")}`")

    return rates


def build_database():
    rates = asyncio.run(fetch())
    db = get_database()

    db.executemany(
        "INSERT INTO rates(time, source, rate) VALUES (?, 'paralelo', ?) ON CONFLICT (time, source) DO NOTHING",
        rates,
    )
    db.commit()
