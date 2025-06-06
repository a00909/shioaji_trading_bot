import threading
from datetime import datetime, timedelta
from typing import Callable

from pandas import DataFrame, to_datetime
from redis.client import Redis
from shioaji.constant import TicksQueryType
from shioaji.data import Ticks

from data.tick_fop_v1d1 import TickFOPv1D1
from tools.constants import DATE_FORMAT_SHIOAJI
from tools.utils import ticks_to_tickfopv1


class IndayHistoryGetter:
    def __init__(self, redis, redis_key: Callable[[], str], api, contract, get_serial: Callable[[], int]):
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



    def check_inday_history(self):
        key = self.redis_key()
        res = self.redis.zrange(key, -2, -1, withscores=False)
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

        key = self.redis_key()

        tickfopv1s = ticks_to_tickfopv1(ticks)

        data = {}
        for tfop in tickfopv1s:
            if tfop.datetime >= self.start_time:
                break
            data[tfop.serialize(self.get_serial())] = tfop.datetime.timestamp()

        self.redis.zadd(key, data)
        print('redis data finished.')
        self.received_count += 1
        print(f'redis data inserted.{self.received_count}/{self.received_total}')

        if self.received_total or self.received_count >= self.received_total:
            self.set_finish()
            print('rtm ready.')

    def wait_for_finish(self):
        self.finish.wait()

    def prepare_in_day_history(self):
        date = self.start_time.date()
        if 15 <= self.start_time.hour <= 23:
            date += timedelta(days=1)

        data: list | None = None
        if not self.last_end_time:
            data = [None]
            self.api.ticks(
                self.contract,
                date.strftime(DATE_FORMAT_SHIOAJI),
                TicksQueryType.AllDay,
                timeout=0,  # 非阻塞 timeout = 0
                cb=self.inday_history_cb
            )
        else:  # todo: check這裡的邏輯, 是否可能會造成換日漏掉資料
            if self.last_end_time.date() == self.start_time.date():
                print(self.last_end_time.date(), self.start_time.date())
                data = [(self.last_end_time.time().isoformat(), self.start_time.time().isoformat())]
            else:
                data = [
                    (self.last_end_time.time().isoformat(), "23:59:59.999999"),
                    ("00:00:00", self.start_time.time().isoformat())
                ]

            for d in data:
                print(f'query range: {d}')
                self.api.ticks(
                    self.contract,
                    date.strftime(DATE_FORMAT_SHIOAJI),
                    TicksQueryType.RangeTime,
                    time_start=d[0],
                    time_end=d[1],
                    timeout=0,  # 非阻塞 timeout = 0
                    cb=self.inday_history_cb
                )

        self.received_total = len(data) if data else 0
