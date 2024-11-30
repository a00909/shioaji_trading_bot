import threading
from datetime import datetime, timedelta
from decimal import Decimal

import shioaji as sj
from redis.client import Redis
import numpy as np
import pandas as pd
from shioaji.backend.solace.api import TicksQueryType
from ta.momentum import RSIIndicator

from utils.common import decode_redis
from utils.constants import DATE_FORMAT_SHIOAJI


class RealtimeTickManager:
    fop_tick_attr_set = {
        # 'code',
        # 'datetime',
        'open',
        'underlying_price',
        'bid_side_total_vol',
        'ask_side_total_vol',
        'avg_price',
        'close',
        'high',
        'low',
        'amount',
        'total_amount',
        'volume',
        'total_volume',
        'tick_type',
        'chg_type',
        'price_chg',
        'pct_chg',
        # 'simtrade',
    }
    fop_bidask_attr_set = {
        'code',
        'datetime',
        'bid_price',
        'bid_volume',
        'diff_bid_vol',
        'ask_price',
        'ask_volume',
        'diff_ask_vol',
        'suspend',
        # 'simtrade',
        'intraday_odd',
    }
    realtime_key_prefix = 'realtime.tick'
    date_format_redis = '%Y.%m.%d'

    def __init__(self, api: sj.Shioaji, redis, contract):
        self.api = api
        self.redis: Redis = redis
        self.api.quote.set_on_tick_fop_v1_callback(self.on_tick_fop_v1_handler)
        self.api.quote.set_on_bidask_fop_v1_callback(self.on_bidask_fop_v1_handler)
        self.contract = contract
        self.symbol = contract.symbol

        self.flush_keys()
        self.started = False
        self.tick_received_event = threading.Event()
        self.start_time: datetime = None

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

        # for test
        # self.ticks: list[sj.TickFOPv1] = []
        # self.api.quote.set_on_tick_stk_v1_callback(self.on_stk_v1_tick_handler)

    def flush_keys(self):
        keys: list[bytes] = self.redis.keys(f'{self.realtime_key_prefix}*')

        pipe = self.redis.pipeline()
        for k in keys:
            pipe.delete(decode_redis(k))

        pipe.execute()

    def start(self):
        if self.started:
            print('already started.')
            return

        for sub in self.subs:
            self.api.quote.subscribe(**sub)
        self.started = True
        print('started.')

    def stop(self):
        if not self.started:
            print('not started yet.')
            return

        for sub in self.subs:
            self.api.quote.unsubscribe(**sub)
        self.started = False
        print('stopped.')

    def prepare_in_day_history(self):
        date = self.start_time.date()
        if not (8 <= self.start_time.hour <= 23):
            date -= timedelta(days=1)

        def cb(ticks:Ticks):
            



        ticks = self.api.ticks(
            self.contract,
            date.strftime(DATE_FORMAT_SHIOAJI),
            TicksQueryType.AllDay,
            timeout=0,
            cb=cb
        )
        pass

    def get_tick_serial(self):
        key = f'{self.realtime_key_prefix}.serial:{self.symbol}'
        return int(self.redis.incr(key))

    def serialize_tick(self, tick: sj.TickFOPv1, separator=':'):
        """
        因為tick是第三方中的元件所以只好寫在這
        :param tick:
        :param separator:
        :return:
        """
        serialized = (
            f"{tick.code}{separator}"
            f"{tick.datetime.timestamp()}{separator}"
            f"{tick.open}{separator}"
            f"{tick.underlying_price}{separator}"
            f"{tick.bid_side_total_vol}{separator}"
            f"{tick.ask_side_total_vol}{separator}"
            f"{tick.avg_price}{separator}"
            f"{tick.close}{separator}"
            f"{tick.high}{separator}"
            f"{tick.low}{separator}"
            f"{tick.amount}{separator}"
            f"{tick.total_amount}{separator}"
            f"{tick.volume}{separator}"
            f"{tick.total_volume}{separator}"
            f"{tick.tick_type}{separator}"
            f"{tick.chg_type}{separator}"
            f"{tick.price_chg}{separator}"
            f"{tick.pct_chg}{separator}"
            f"{1 if tick.simtrade else 0}{separator}"
            f"{self.get_tick_serial()}"
        )
        return serialized

    def deserialize_tick(self, data: bytes, separator=':'):
        values = decode_redis(data).split(separator)

        tick = sj.TickFOPv1()

        tick.code = values[0]
        tick.datetime = datetime.fromtimestamp(float(values[1]))
        tick.open = Decimal(values[2])
        tick.underlying_price = Decimal(values[3])
        tick.bid_side_total_vol = int(values[4])
        tick.ask_side_total_vol = int(values[5])
        tick.avg_price = Decimal(values[6])
        tick.close = Decimal(values[7])
        tick.high = Decimal(values[8])
        tick.low = Decimal(values[9])
        tick.amount = Decimal(values[10])
        tick.total_amount = Decimal(values[11])
        tick.volume = int(values[12])
        tick.total_volume = int(values[13])
        tick.tick_type = int(values[14])
        tick.chg_type = int(values[15])
        tick.price_chg = Decimal(values[16])
        tick.pct_chg = Decimal(values[17])
        tick.simtrade = values[18] == 1

        return tick

    def redis_key(self):
        return f'{self.realtime_key_prefix}:{self.symbol}:{self.get_date_tag().strftime(self.date_format_redis)}'

    def on_tick_fop_v1_handler(self, _exchange: sj.Exchange, tick: sj.TickFOPv1):
        pipe = self.redis.pipeline()

        key = self.redis_key()
        pipe.zadd(key, {self.serialize_tick(tick): tick.datetime.timestamp()})
        pipe.execute()

        self.tick_received_event.set()

        if not self.start_time:
            self.start_time = tick.datetime
            self.prepare_in_day_history()

    def on_bidask_fop_v1_handler(self, _exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        print(f'[bidask_handler] {bidask}')

    def get_date_tag(self):
        now_dt = datetime.now()
        if 0 <= now_dt.hour <= 5:
            return now_dt.date() - timedelta(days=1)
        else:
            return now_dt.date()

    def get_ticks_by_backtracking_time(self, backtracking_time: timedelta) -> list[sj.TickFOPv1]:
        end = datetime.now()
        start = end - backtracking_time

        pipe = self.redis.pipeline()

        key = self.redis_key()
        pipe.zrangebyscore(key, start.timestamp(), end.timestamp(), withscores=False)

        results = pipe.execute()[0]

        return [self.deserialize_tick(tick) for tick in results]

    def get_ticks_by_backward_idx(self, backward_idx=0) -> list[sj.TickFOPv1]:
        data = self.redis.zrange(self.redis_key(), -1 - backward_idx, -1)

        return [self.deserialize_tick(tick) for tick in data]

    def get_ticks_by_index(self, start=None, end=None):
        data = self.redis.zrange(self.redis_key(), start=start, end=end)

        return [self.deserialize_tick(tick) for tick in data]
