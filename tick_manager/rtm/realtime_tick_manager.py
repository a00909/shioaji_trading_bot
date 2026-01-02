import threading
from datetime import datetime, timedelta

import shioaji as sj
from redis.client import Redis
from shioaji import TickFOPv1
from typing_extensions import override

from data.unified.bid_ask.bid_ask_fop import BidAskFOP
from data.unified.tick.tick_fop import TickFOP
from tick_manager.rtm.rtm_base import RealtimeTickManagerBase
from tick_manager.rtm_extensions.inday_history_getter import IndayHistoryGetter
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import decode_redis, get_redis_date_tag, get_serial


class RealtimeTickManager(RealtimeTickManagerBase):
    realtime_key_prefix = 'realtime.tick'

    def __init__(self, api: sj.Shioaji, redis, contract, getting_history=True):

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
            {
                'contract': self.contract,
                'quote_type': sj.constant.QuoteType.BidAsk,
                'version': sj.constant.QuoteVersion.v1,
            },

        ]
        self.need_lock = True
        self.buffer_lock = threading.Lock()
        self.new_data_index = -1
        self.skip_combine = False

        self.last_total_volume = None

        self.getting_history = getting_history

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
        if self.skip_combine or not self.getting_history:
            return

        self._combine_data()

    def _combine_data(self):
        in_day_history = self.ihg.get_data()
        older_raw = self.redis.zrangebyscore(  # 實際只會抓到last_end
            self._redis_key(),
            (self.start_time - self.window_size).timestamp(),
            self.start_time.timestamp(),
            withscores=False
        )
        older_data = [TickFOP.deserialize(r) for r in older_raw]

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
            buffer_size = len(self.tick_buffer)
            while buffer_start < buffer_size and self.tick_buffer[buffer_start].datetime <= in_day_end:
                buffer_start += 1
            self.tick_buffer = older_data[:older_right + 1] + in_day_history + self.tick_buffer[buffer_start:]
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

    def _check_tick_validity(self, tick, tickv1d1):
        # print delay
        now = datetime.now(tz=DEFAULT_TIMEZONE)
        if not self.last_print_delay or now - self.last_print_delay > timedelta(seconds=10):
            delay = (now - tick.datetime).total_seconds()
            print(f'[realtime tick delay] {delay} s.')
            self.last_print_delay = now

        # check tick miss
        if self.last_total_volume and tickv1d1.volume + self.last_total_volume != tickv1d1.total_volume:
            print(
                f'[tick miss detected] '
                f't_vol_total_last={self.last_total_volume}, '
                f't_vol_total={tickv1d1.total_volume}, '
                f't_vol={tickv1d1.volume}'
            )
        self.last_total_volume = tickv1d1.total_volume

    def _deal_inday_history(self, tickv1d1):
        # in-day history ticks
        if not self.start_time:
            self.ihg.set_start_time(tickv1d1.datetime)
            self.ihg.check_inday_history()
            if tickv1d1.volume == tickv1d1.total_volume or not self.getting_history:
                self.ihg.set_finish()
                self.skip_combine = True
                self.need_lock = False
                print('rtm ready.')
            else:
                self.ihg.prepare_in_day_history()

    def _on_tick_fop_v1_handler(self, _exchange: sj.Exchange, tick: TickFOPv1):
        tick.datetime = tick.datetime.replace(tzinfo=DEFAULT_TIMEZONE)

        tickv1d1 = TickFOP.from_sj(tick)

        self._check_tick_validity(tick, tickv1d1)

        # append to buffer
        if self.need_lock:
            with self.buffer_lock:
                self.tick_buffer.append(tickv1d1)
        else:
            self.tick_buffer.append(tickv1d1)

        self.tick_received_event.set()

        self._deal_inday_history(tickv1d1)

    def _on_bidask_fop_v1_handler(self, _exchange: sj.Exchange, bidask: sj.BidAskFOPv1):
        bidaskvidi = BidAskFOP.bidaskv1_to_v1d1(bidask)
        self.bid_ask_buffer.append(bidaskvidi)

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

    @override
    def update_window_right(self):
        self.tick_right = len(self.tick_buffer) - 1
        self.bid_ask_right = len(self.bid_ask_buffer) - 1

    @override
    def wait_for_tick(self):
        self.tick_received_event.wait(timeout=10)
        if not self.started:
            return False
        self.tick_received_event.clear()

        self.update_window()

        return True

    def _dump_to_redis(self):
        key = self._redis_key()
        data = {
            t.serialize(self._get_tick_serial()): t.datetime.timestamp()
            for t in self.tick_buffer[self.new_data_index:]
        }
        self.redis.zadd(key, data)
