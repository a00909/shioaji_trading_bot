import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import shioaji as sj
from redis.client import Redis
import numpy as np
import pandas as pd
from shioaji import TickFOPv1
from shioaji.backend.solace.api import TicksQueryType
from shioaji.data import Ticks
from ta.momentum import RSIIndicator

from data.tick_fop_v1d1 import TickFOPv1D1
from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tick_manager.rtm_extensions.inday_history_getter import IndayHistoryGetter
from tools.utils import decode_redis, history_ts_to_datetime, get_now, default_tickfopv1, ticks_to_tickfopv1
from tools.constants import DATE_FORMAT_SHIOAJI, DEFAULT_TIMEZONE


class RealtimeTickManager:
    realtime_key_prefix = 'realtime.tick'
    date_format_redis = '%Y.%m.%d'

    def __init__(self, api: sj.Shioaji, redis, contract):
        self.api = api
        self.redis: Redis = redis
        self.api.quote.set_on_tick_fop_v1_callback(self.on_tick_fop_v1_handler)
        self.api.quote.set_on_bidask_fop_v1_callback(self.on_bidask_fop_v1_handler)
        self.contract = contract
        self.symbol = contract.symbol

        # self.flush_keys()
        self.started = False
        self.tick_received_event = threading.Event()

        # 目前只用於補齊in-day history
        self.start_time: datetime = None

        self.last_end_time: datetime = None

        self.last_print_delay: datetime = None

        # extensions
        self.btg = BacktrackingTimeGetter(self.redis, self.redis_key)
        self.ihg = IndayHistoryGetter(self.redis, self.redis_key, self.api, self.contract, self.get_tick_serial)

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

    def start(self):
        if self.started:
            print('rtm already started.')
            return
        self.ihg.check_inday_history()

        for sub in self.subs:
            self.api.quote.subscribe(**sub)
        self.started = True
        print('rtm started.')

    def stop(self):
        if not self.started:
            print('rtm not started yet.')
            return

        for sub in self.subs:
            self.api.quote.unsubscribe(**sub)
        self.started = False
        self.tick_received_event.set()  # for those who may wait for the event to stop
        print('rtm stopped.')

        # event handler

    def wait_for_ready(self):
        self.ihg.wait_for_finish()

    def on_tick_fop_v1_handler(self, _exchange: sj.Exchange, tick: TickFOPv1):
        key = self.redis_key()

        tick.datetime = tick.datetime.replace(tzinfo=DEFAULT_TIMEZONE)

        tickv1d1 = TickFOPv1D1.tickfopv1_to_v1d1(tick)

        self.redis.zadd(key, {tickv1d1.serialize(self.get_tick_serial()): tickv1d1.datetime.timestamp()})

        now = datetime.now()
        if not self.last_print_delay or now - self.last_print_delay > timedelta(seconds=10):
            delay = (datetime.now(tz=DEFAULT_TIMEZONE) - tick.datetime).total_seconds()
            print(f'[Realtime tick delay] {delay} s.\n')
            self.last_print_delay = now

        self.tick_received_event.set()

        if not self.ihg.start_time:
            if tickv1d1.volume != tickv1d1.total_volume:  # 非第一根tick
                self.ihg.set_start_time(tickv1d1.datetime)
                self.ihg.prepare_in_day_history()
            else:
                self.ihg.set_finish()
                print('rtm ready.')

    def on_bidask_fop_v1_handler(self, _exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        print(f'[bidask_handler] {bidask}')

    # redis
    def redis_key(self):
        return f'{self.realtime_key_prefix}:{self.symbol}:{self.get_date_tag().strftime(self.date_format_redis)}'

    def flush_keys(self):
        keys: list[bytes] = self.redis.keys(f'{self.realtime_key_prefix}*')

        pipe = self.redis.pipeline()
        for k in keys:
            pipe.delete(decode_redis(k))

        pipe.execute()

    def get_tick_serial(self):
        key = f'{self.realtime_key_prefix}.serial:{self.symbol}'
        return int(self.redis.incr(key))

    def get_date_tag(self):
        now_dt = get_now()
        if 15 <= now_dt.hour <= 23:
            return now_dt.date() + timedelta(days=1)
        return now_dt.date()

    # functions
    def wait_for_tick(self):
        self.tick_received_event.wait(timeout=10)
        self.tick_received_event.clear()
        return True

    def get_ticks_by_backtracking_time(self, backtracking_time: timedelta) -> list[TickFOPv1]:
        end = get_now()
        start = end - backtracking_time

        results = self.btg.get(start, end)

        return results

    def get_ticks_by_backward_idx(self, backward_idx=0) -> list[sj.TickFOPv1]:
        data = self.redis.zrange(self.redis_key(), -1 - backward_idx, -1)

        return [TickFOPv1D1.deserialize(tick) for tick in data]

    def get_ticks_by_index(self, start=0, end=-1):
        data = self.redis.zrange(self.redis_key(), start=start, end=end)

        return [TickFOPv1D1.deserialize(tick) for tick in data]
