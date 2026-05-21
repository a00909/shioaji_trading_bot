from datetime import datetime, timedelta

SJ_OFFSET_S = 28_800
SJ_OFFSET_US = SJ_OFFSET_S * 1_000_000
PG_EPOCH_OFFSET_S = 946_684_800
PG_EPOCH_OFFSET_US = PG_EPOCH_OFFSET_S * 1_000_000
PG_EPOCH_WITH_TZ_S = PG_EPOCH_OFFSET_S + SJ_OFFSET_S
PG_EPOCH_WITH_TZ_US = PG_EPOCH_OFFSET_US + SJ_OFFSET_US


def sj_history_ns_to_datetime(ts: int):
    ts //= 10 ** 9
    ts -= SJ_OFFSET_S
    dt = datetime.fromtimestamp(ts)
    return dt


def pg_us_to_unix_seconds(pg_us: int):
    return pg_us // (10 ** 6) + PG_EPOCH_OFFSET_S


def pg_us_to_datetime(pg_us: int):
    unix_seconds = pg_us_to_unix_seconds(pg_us)
    return datetime.fromtimestamp(unix_seconds)


def sj_history_ns_to_pg_us(sj_ns: int):
    us = sj_ns // 1000  # 轉微秒
    us -= PG_EPOCH_WITH_TZ_US
    return us


def datetime_to_sj_ns(dt: datetime):
    ts = dt.timestamp()
    ts += SJ_OFFSET_S
    ts *= 10 ** 9
    return ts


def datetime_to_pg_us(dt: datetime):
    ts = dt.timestamp()
    ts -= PG_EPOCH_OFFSET_S
    ts *= 10 ** 6
    return ts
