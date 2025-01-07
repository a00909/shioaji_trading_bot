import threading
from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import shioaji as sj
from redis.client import Redis
import numpy as np
import pandas as pd
from selenium.webdriver.common.devtools.v85.debugger import resume
from shioaji import TickFOPv1
from shioaji.backend.solace.api import TicksQueryType
from shioaji.data import Ticks
from ta.momentum import RSIIndicator

from data.tick_fop_v1d1 import TickFOPv1D1
from tick_manager.rtm.rtm_base import RealtimeTickManagerBase
from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tick_manager.rtm_extensions.inday_history_getter import IndayHistoryGetter
from tools.utils import decode_redis, history_ts_to_datetime, get_now, default_tickfopv1, ticks_to_tickfopv1, \
    get_twse_date, get_redis_date_tag, get_serial
from tools.constants import DATE_FORMAT_SHIOAJI, DEFAULT_TIMEZONE, DATE_FORMAT_REDIS


class RealtimeTickManager(RealtimeTickManagerBase):
    realtime_key_prefix = 'realtime.tick'

    def __init__(self, api: sj.Shioaji, redis, contract):

        super().__init__()
        self.api = api
        self.redis: Redis = redis
        self.api.quote.set_on_tick_fop_v1_callback(self._on_tick_fop_v1_handler)
        self.api.quote.set_on_bidask_fop_v1_callback(self._on_bidask_fop_v1_handler)
        self.contract = contract
        self.symbol = contract.symbol

        # self.flush_keys()
        self.started = False
        self.tick_received_event = threading.Event()

        self.last_print_delay: datetime = None

        # extensions
        # self.btg = BacktrackingTimeGetter(self.redis, self.__redis_key)
        self.ihg = IndayHistoryGetter(
            self.redis,
            self._redis_key,
            self.api,
            self.contract,
            self._get_tick_serial,
            self.window_size
        )

        self.subs = [
            {
                'contract': self.contract,
                'quote_type': sj.constant.QuoteType.Tick,
                'version': sj.constant.QuoteVersion.v1,
            },
            # {
            #     'contract': app.api.Contracts.Futures.TMF.TMFR1,
            #     'quote_type': sj.constant.QuoteType.BidAsk,
            #     'version': sj.constant.QuoteVersion.v1,
            # },

        ]
        self.need_lock = True
        self.buffer_lock = threading.Lock()
        self.new_data_index = -1

    def start(self, wait_for_ready=True):
        if self.started:
            print('rtm already started.')
            return

        for sub in self.subs:
            self.api.quote.subscribe(**sub)
        self.started = True
        print('rtm started. waiting for ready...')

        if wait_for_ready:
            self.wait_for_ready()
            print('rtm ready.')

    def stop(self):
        if not self.started:
            print('rtm not started yet.')
            return

        for sub in self.subs:
            self.api.quote.unsubscribe(**sub)
        self.started = False
        self.tick_received_event.set()  # for those who may wait for the event to stop

        self._dump_to_redis()
        print('rtm stopped.')

        # event handler

    def wait_for_ready(self):
        self.ihg.wait_for_finish()
        self._combine_data()

    def _combine_data(self):
        in_day_history = self.ihg.get_data()
        older_raw = self.redis.zrangebyscore(  # 實際只會抓到last_end
            self._redis_key(),
            (self.start_time - self.window_size).timestamp(),
            self.start_time.timestamp(),
            withscores=False
        )
        older_data = [TickFOPv1D1.deserialize(r) for r in older_raw]

        # to remove duplicated parts at the end
        in_day_start = in_day_history[0].datetime
        older_right = len(older_data) - 1
        rm_count = 0
        while older_right >= 0 and older_data[older_right].datetime >= in_day_start:
            older_right -= 1
            rm_count += 1

        # to remove duplicated parts at the start of buffer
        in_day_end = in_day_history[-1].datetime
        buffer_start = 0

        with self.buffer_lock:
            buffer_size = len(self.buffer)
            while buffer_start < buffer_size and self.buffer[buffer_start].datetime <= in_day_end:
                buffer_start += 1
            self.buffer = older_data[:older_right + 1] + in_day_history + self.buffer[buffer_start:]
            self.need_lock = False

        if rm_count > 0:
            self.redis.zremrangebyrank(
                self._redis_key(),
                -rm_count,
                -1
            )
        self.new_data_index = older_right + 1

    @property
    def start_time(self) -> datetime:
        return self.ihg.start_time

    def _on_tick_fop_v1_handler(self, _exchange: sj.Exchange, tick: TickFOPv1):
        tick.datetime = tick.datetime.replace(tzinfo=DEFAULT_TIMEZONE)

        tickv1d1 = TickFOPv1D1.tickfopv1_to_v1d1(tick)

        if self.need_lock:
            with self.buffer_lock:
                self.buffer.append(tickv1d1)
        else:
            self.buffer.append(tickv1d1)

        # print delay
        now = datetime.now(tz=DEFAULT_TIMEZONE)
        if not self.last_print_delay or now - self.last_print_delay > timedelta(seconds=10):
            delay = (now - tick.datetime).total_seconds()
            print(f'[Realtime tick delay] {delay} s.\n')
            self.last_print_delay = now

        self.tick_received_event.set()

        # in-day history ticks
        if not self.start_time:
            self.ihg.set_start_time(tickv1d1.datetime)
            self.ihg.check_inday_history()
            if tickv1d1.volume != tickv1d1.total_volume:  # 非第一根tick
                self.ihg.prepare_in_day_history()
            else:
                self.ihg.set_finish()
                print('rtm ready.')

    def _on_bidask_fop_v1_handler(self, _exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        print(f'[bidask_handler] {bidask}')

    # redis
    def _redis_key(self):
        return f'{self.realtime_key_prefix}:{self.symbol}:{self._get_date_tag()}'

    def _flush_keys(self):
        keys: list[bytes] = self.redis.keys(f'{self.realtime_key_prefix}*')

        pipe = self.redis.pipeline()
        for k in keys:
            pipe.delete(decode_redis(k))

        pipe.execute()

    def _get_tick_serial(self):
        key = f'{self.realtime_key_prefix}.serial:{self.symbol}'
        return get_serial(self.redis, key)

    def _get_date_tag(self):
        # return get_redis_date_tag(self.start_time if self.start_time else get_now())
        return get_redis_date_tag(self.start_time)

    # functions
    def wait_for_tick(self):
        self.tick_received_event.wait(timeout=10)
        if not self.started:
            return False
        self.tick_received_event.clear()

        self.window_right = len(self.buffer) - 1
        self._update_window_left()

        return True

    def _dump_to_redis(self):
        key = self._redis_key()
        data = {
            t.serialize(self._get_tick_serial()): t.datetime.timestamp()
            for t in self.buffer[self.new_data_index:]
        }
        self.redis.zadd(key, data)
