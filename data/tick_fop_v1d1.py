from datetime import datetime
from decimal import Decimal

from shioaji import TickFOPv1

from tools.constants import DEFAULT_TIMEZONE
from tools.utils import decode_redis


class TickFOPv1D1(TickFOPv1):
    def __init__(self):
        pass

    @classmethod
    def tickfopv1_to_v1d1(cls, tick: TickFOPv1):
        tickv1d1 = cls()
        tickv1d1.code = tick.code
        tickv1d1.datetime = tick.datetime
        tickv1d1.open = tick.open
        tickv1d1.underlying_price = tick.underlying_price
        tickv1d1.bid_side_total_vol = tick.bid_side_total_vol
        tickv1d1.ask_side_total_vol = tick.ask_side_total_vol
        tickv1d1.avg_price = tick.avg_price
        tickv1d1.close = tick.close
        tickv1d1.high = tick.high
        tickv1d1.low = tick.low
        tickv1d1.amount = tick.amount
        tickv1d1.total_amount = tick.total_amount
        tickv1d1.volume = tick.volume
        tickv1d1.total_volume = tick.total_volume
        tickv1d1.tick_type = tick.tick_type
        tickv1d1.chg_type = tick.chg_type
        tickv1d1.price_chg = tick.price_chg
        tickv1d1.pct_chg = tick.pct_chg
        tickv1d1.simtrade = tick.simtrade

        return tickv1d1

    @classmethod
    def deserialize(cls, data: bytes, separator=':'):
        values = decode_redis(data).split(separator)
        tick = cls()
        tick.code = values[0]
        tick.datetime = datetime.fromtimestamp(float(values[1]), tz=DEFAULT_TIMEZONE)
        tick.open = Decimal(values[2])
        tick.underlying_price = Decimal(values[3])
        tick.bid_side_total_vol = int(values[4])
        tick.ask_side_total_vol = int(values[5])
        tick.avg_price = Decimal(values[6])
        tick.close = float(values[7])
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

    def serialize(self, serial_num=-1, separator=':'):
        serialized = (
            f"{self.code}{separator}"
            f"{self.datetime.timestamp()}{separator}"
            f"{self.open}{separator}"
            f"{self.underlying_price}{separator}"
            f"{self.bid_side_total_vol}{separator}"
            f"{self.ask_side_total_vol}{separator}"
            f"{self.avg_price}{separator}"
            f"{self.close}{separator}"
            f"{self.high}{separator}"
            f"{self.low}{separator}"
            f"{self.amount}{separator}"
            f"{self.total_amount}{separator}"
            f"{self.volume}{separator}"
            f"{self.total_volume}{separator}"
            f"{self.tick_type}{separator}"
            f"{self.chg_type}{separator}"
            f"{self.price_chg}{separator}"
            f"{self.pct_chg}{separator}"
            f"{1 if self.simtrade else 0}{separator}"
            f"{serial_num}"
        )
        return serialized

    def __lt__(self, other):
        if isinstance(other, datetime):
            return self.datetime < other
        elif isinstance(other, TickFOPv1D1):
            return self.datetime < other.datetime
        return NotImplemented

    def __eq__(self, other):
        if isinstance(other, datetime):
            return self.datetime == other
        elif isinstance(other, TickFOPv1D1):
            return self.datetime == other.datetime
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, datetime):
            return self.datetime > other
        elif isinstance(other, TickFOPv1D1):
            return self.datetime > other.datetime
        return NotImplemented

    def __repr__(self):
        attributes = ', '.join(f"{key}={repr(value)}" for key, value in self.__dict__.items())
        return f"{self.__class__.__name__}({attributes})"
