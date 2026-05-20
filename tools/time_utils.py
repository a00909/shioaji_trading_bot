from datetime import datetime

from tools.constants import UTC_TZ, DEFAULT_TIMEZONE


def sj_history_ns_to_datetime(ts: int):
    ts_posix = ts / (10 ** 9)
    return datetime.fromtimestamp(ts_posix, tz=UTC_TZ).replace(tzinfo=DEFAULT_TIMEZONE)


def pg_us_to_datetime(ts: int):
    ts += 946_684_800_000_000  # pg epoch 轉 unix
    ts_posix = ts / (10 ** 6)  # to seconds
    return datetime.fromtimestamp(ts_posix)


def sj_history_ns_to_pg_us(sj_ns: int):
    us = sj_ns // 1000  # 轉微秒
    us -= 946_684_800_000_000  # 轉 pg epoch
    us -= 28_800_000_000  # +8 轉 utc

    return us


def datetime_to_pg_us(dt: datetime):
    us = dt.timestamp() * 10 ** 6
    us -= 946_684_800_000_000  # 轉 pg epoch
    return us
