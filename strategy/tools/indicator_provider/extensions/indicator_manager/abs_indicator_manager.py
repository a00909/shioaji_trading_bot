from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from functools import cache
from typing import Final

from redis.client import Redis

from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager
from tools.utils import get_redis_date_tag, get_serial, get_by_time_range


class AbsIndicatorManager(ABC):
    serial_key_prefix = 'indicator_serial'
    storage_key_prefix = 'indicator'
    MAX_BUFFER_SIZE: Final[int] = 131072

    def __init__(self, indicator_type, length, symbol: str, start_time, redis: Redis, rtm):
        self.indicator_type: Final[IndicatorType] = indicator_type
        self.length: Final[timedelta] = length
        self.symbol: Final[str] = symbol
        self.buffer: list[Indicator] = []
        self.change_rate_buffer: list[float] = []

        self.redis = redis
        self.start_time: datetime = start_time
        self.rtm: RealtimeTickManager = rtm

    def last(self):
        if self.buffer:
            return self.buffer[-1]
        else:
            return None

    def is_valid_last(self, now, last: Indicator):
        if last.datetime <= now - self.length:
            return False
        return True

    def update(self, now):
        last = self.last()
        if last and not self.is_valid_last(now, last):
            last = None
        new = self.calculate(now, last)

        if new:
            self.dump_to_redis()
            self.buffer.append(new)

    def change_rate(self, window_size=None):
        """
        方法:找窗口的左側，用最新值(右側)減掉，除以間隔數
        :return:
        """
        if not (self.buffer and len(self.buffer) >= 2):
            return 0

        if not window_size:
            window_size = self.length

        prev_index = get_by_time_range(
            self.buffer,
            0,
            len(self.buffer),
            self.last().datetime - window_size
        )

        if self.get(prev_index) == 0:
            return 0

        change_rate = (self.get() - self.get(prev_index))
        return change_rate

    @abstractmethod
    def calculate(self, now: datetime, last: Indicator) -> Indicator:
        pass

    @cache
    def get_key_postfix(self):
        # tmf:ma30s:2024.12.12:
        return f'{self.symbol}:{self.indicator_type.value}{self.length.total_seconds()}s:{get_redis_date_tag(self.start_time)}'

    @cache
    def get_serial_key(self):
        return f'{self.serial_key_prefix}:{self.get_key_postfix()}'

    @cache
    def get_storage_key(self):
        return f'{self.storage_key_prefix}:{self.get_key_postfix()}'

    def dump_to_redis(self, anyway=False):
        if anyway:
            data = {
                idc.serialize(get_serial(self.redis, self.get_serial_key())): idc.datetime.timestamp()
                for idc in self.buffer
            }
        elif len(self.buffer) > self.MAX_BUFFER_SIZE:
            data = {
                idc.serialize(get_serial(self.redis, self.get_serial_key())): idc.datetime.timestamp()
                for idc in self.buffer[:self.MAX_BUFFER_SIZE / 2]
            }
            self.buffer = self.buffer[self.MAX_BUFFER_SIZE / 2:]
        else:
            return

        self.redis.zadd(
            self.get_storage_key(),
            data
        )

    def clear_buffer(self):
        self.buffer.clear()

    def get(self, idx=-1, return_indicator=False):
        indicator = self.buffer[idx] if abs(idx) + (1 if idx > 0 else 0) <= len(self.buffer) else None

        if not indicator:
            return None
        if return_indicator:
            return indicator
        return indicator.get()
