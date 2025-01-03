import threading
from datetime import datetime, timedelta
from typing import Callable

from pandas import DataFrame, to_datetime
from redis.client import Redis
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks

from data.tick_fop_v1d1 import TickFOPv1D1
from tools.constants import DATE_FORMAT_SHIOAJI
from tools.utils import ticks_to_tickfopv1, get_twse_date


class IndayHistoryGetter:
    def __init__(self, redis, redis_key: Callable[[], str], api, contract, get_serial: Callable[[], int], window_size):
        self.redis: Redis = redis
        self.redis_key = redis_key
        self.last_end_time: datetime = None
        self.api = api
        self.contract = contract
        self.start_time = None
        self.finish = threading.Event()
        self.get_serial = get_serial

        self.received_count = 0
        self.received_total = 0

        self.buffer: list[TickFOPv1D1] = []
        self.window_size = window_size

    def check_inday_history(self):
        key = self.redis_key()
        res = self.redis.zrange(key, -1, -1, withscores=False)
        if res:
            last_tick = TickFOPv1D1.deserialize(res[0])
            self.last_end_time = last_tick.datetime

    def print_ticks(self, ticks):
        df = DataFrame({**ticks})
        df.ts = to_datetime(df.ts)
        print(df)

    def set_start_time(self, start):
        self.start_time = start

    def set_finish(self):
        self.finish.set()

    def inday_history_cb(self, ticks: Ticks):
        print('in-day history received.')
        self.print_ticks(ticks)

        # key = self.redis_key()

        tickfopv1s = ticks_to_tickfopv1(ticks)

        self.buffer.extend(tickfopv1s)
        self.received_count += 1
        print(f'data received. {self.received_count}/{self.received_total}')

        if self.received_total or self.received_count >= self.received_total:
            self.set_finish()
            print('rtm ready.')

    def wait_for_finish(self):
        self.finish.wait()

    def prepare_in_day_history(self):
        # normalize_date
        query_date = get_twse_date(self.start_time)

        if self.last_end_time:
            query_start = max(self.last_end_time, self.start_time - self.window_size)
        else:
            query_start = self.start_time - self.window_size
        query_end = self.start_time

        # todo: check這裡的邏輯, 是否可能會造成換日漏掉資料
        print('in day query: ',query_start.date(), query_end.date())
        if query_start.date() == query_end.date():

            data = [(query_start.time().isoformat(), query_end.time().isoformat())]
        else:
            data = [
                (query_start.time().isoformat(), "23:59:59.999999"),
                ("00:00:00", query_end.time().isoformat())
            ]

        for d in data:
            print(f'query range: {d}')
            self.api.ticks(
                self.contract,
                query_date.strftime(DATE_FORMAT_SHIOAJI),
                TicksQueryType.RangeTime,
                time_start=d[0],
                time_end=d[1],
                timeout=0,  # 非阻塞 timeout = 0
                cb=self.inday_history_cb
            )

        self.received_total = len(data) if data else 0

    def get_data(self):
        return self.buffer
