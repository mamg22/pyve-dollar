import datetime

VE_TZ = datetime.timezone(-datetime.timedelta(hours=4))

REDENOMINATION_DAY = datetime.datetime(2021, 10, 1, tzinfo=VE_TZ)
REDENOMINATION_FACTOR = 1_000_000
