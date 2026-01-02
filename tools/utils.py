import logging
import os
import time
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from datetime import time as datetime_time
from decimal import Decimal

import pandas as pd
import shioaji as sj
from dotenv import load_dotenv
from redis.client import Redis
from shioaji.contracts import FetchStatus
from shioaji.data import Ticks

from data.unified.bid_ask.bid_ask_fop import BidAskFOP
from tools.constants import DEFAULT_TIMEZONE, UTC_TZ, DATE_FORMAT_REDIS
from tools.custom_logging_formatter import CustomFormatter
from tools.serial_manager import serial_manager

load_dotenv()


def hello() -> None:
    print("Hello from sj-trading!")


def show_version() -> str:
    print(f"Shioaji Version: {sj.__version__}")
    return sj.__version__


def get_shioaji_client() -> sj.Shioaji:
    api = sj.Shioaji()
    print("Shioaji API created")
    return api


def get_api(simulation: bool = True) -> sj.Shioaji:
    api = sj.Shioaji(simulation=simulation)
    api.login(
        api_key=os.environ["API_KEY"],
        secret_key=os.environ["SECRET_KEY"],
    )
    api.activate_ca(
        ca_path=os.environ["CA_CERT_PATH"],
        ca_passwd=os.environ["CA_PASSWORD"],
    )
    while api.Contracts.status == FetchStatus.Fetching:
        print(f'Contracts status: {api.Contracts.status}')
        time.sleep(1)

    return api


def decode_redis(data: bytes) -> str:
    return data.decode()


def history_ts_to_datetime(ts: int):
    ts_posix = ts / (10 ** 9)
    return datetime.fromtimestamp(ts_posix, tz=UTC_TZ).replace(tzinfo=DEFAULT_TIMEZONE)


def get_now():
    return datetime.now(tz=DEFAULT_TIMEZONE)


def tick_to_dict(tick: sj.TickFOPv1):
    return {
        'code': tick.code,
        'datetime': tick.datetime.isoformat(),  # 將datetime轉為ISO格式字串
        'open': str(tick.open),  # 將Decimal轉為字串以便於顯示
        'underlying_price': str(tick.underlying_price),
        'bid_side_total_vol': tick.bid_side_total_vol,
        'ask_side_total_vol': tick.ask_side_total_vol,
        'avg_price': str(tick.avg_price),
        'close': str(tick.close),
        'high': str(tick.high),
        'low': str(tick.low),
        'amount': str(tick.amount),
        'total_amount': str(tick.total_amount),
        'volume': tick.volume,
        'total_volume': tick.total_volume,
        'tick_type': tick.tick_type,
        'chg_type': tick.chg_type,
        'price_chg': str(tick.price_chg),
        'pct_chg': str(tick.pct_chg),
        'simtrade': tick.simtrade,
    }


def to_df(ticks: list[sj.TickFOPv1]):
    data = [tick_to_dict(tick) for tick in ticks]

    # 設定顯示所有行
    pd.set_option('display.max_rows', 10)

    # 設定顯示所有列而不自動換行
    pd.set_option('display.max_colwidth', None)

    # 設定顯示所有列
    pd.set_option('display.max_columns', None)

    df = pd.DataFrame(data)
    return df


def init_custom_logger():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    ch.setFormatter(CustomFormatter())

    logger = logging.getLogger()
    logger.addHandler(ch)

    logger.setLevel(logging.INFO)


def ticks_to_tickfopv1(ticks: Ticks):
    from data.unified.tick.tick_fop import TickFOP

    i = 0
    l = len(ticks.close)
    ret = []
    while i < l:
        dt = history_ts_to_datetime(ticks.ts[i])
        tick = TickFOP(
            datetime=dt,
            close=ticks.close[i],
            volume=ticks.volume[i],
            bid_side_total_vol=ticks.bid_volume[i],
            ask_side_total_vol=ticks.ask_volume[i],
            tick_type=ticks.tick_type[i],
        )
        ret.append(tick)
        i += 1
    return ret


def get_twse_date(dt: datetime):
    """
    轉為證交所的date\n
    邏輯:從前一天夜盤開始就算隔天的日期\n
    (ex: 1/1 星期一 15:00 的資料需要用 1/2去query)
    :param dt:
    :return:
    """
    if 15 <= dt.hour <= 23:  # 夜盤過夜前
        if dt.weekday() == 4:  # 週五
            return dt.date() + timedelta(days=3)
        return dt.date() + timedelta(days=1)
    elif dt.weekday() == 5:  # 週六,只有夜盤,都+2
        return dt.date() + timedelta(days=2)
    return dt.date()


def get_redis_date_tag(dt: datetime):
    return get_twse_date(dt).strftime(DATE_FORMAT_REDIS)


def get_serial(redis: Redis, key):
    return serial_manager.get(key)


def deviation(val1, val2):
    return abs(val1 - val2) / val1


def error(val1, val2, tolerance=0.0001):
    if val1 == 0:
        return False
    if deviation(val1, val2) > tolerance:
        raise Exception(f'deviation exceeded tolerance: {val1}, {val2}')
    return None


def is_valid_range(buffer, left, right):
    return len(buffer) and right <= len(buffer)


def get_by_time_range(
        buffer,
        lo,
        hi,
        start: datetime,
        end: datetime = None,
        with_start=True,
        with_end=True,
        index_only=False
) -> list | tuple | int:
    """
    bisect_right會找到右側插入點的位置; bisect_left則是左側插入點
    :param buffer:
    :param lo:
    :param hi:
    :param start:
    :param end:
    :param with_start:
    :param with_end:
    :param index_only:
    :return:
    """
    ranges = (lo, hi)
    if not is_valid_range(buffer, lo, hi):
        return []

    if with_start:
        left = bisect_left(buffer, start, *ranges)
    else:
        left = bisect_right(buffer, start, *ranges)

    if end:
        if with_end:
            right = bisect_right(buffer, end, *ranges)
        else:
            right = bisect_left(buffer, end, *ranges)

        if index_only:
            return left, right
        return buffer[left:right]

    return left


def is_in_time_ranges(current_time: datetime_time, ranges: list[tuple[datetime_time, datetime_time]]):
    for start, end in ranges:
        if (
                (end < start and (current_time >= start or current_time <= end)) or
                (end > start and (start <= current_time <= end))
        ):
            return True

    return False


def replace_time(dt: datetime, t: datetime_time):
    return dt.replace(hour=t.hour, minute=t.minute, second=t.second, microsecond=t.microsecond)
