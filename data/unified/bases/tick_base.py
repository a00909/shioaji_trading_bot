from abc import ABC
from dataclasses import dataclass
from decimal import Decimal

from data.unified.bases.base import Base


@dataclass(frozen=True, slots=True)
class TickBase(Base, ABC):
    open: Decimal = Decimal(-1)
    bid_side_total_vol: int = -1
    ask_side_total_vol: int = -1
    avg_price: Decimal = Decimal(-1)
    close: float = -1
    high: Decimal = Decimal(-1)
    low: Decimal = Decimal(-1)
    amount: Decimal = Decimal(-1)
    total_amount: Decimal = Decimal(-1)
    volume: int = -1
    total_volume: int = -1
    tick_type: int = -1
    chg_type: int = -1
    price_chg: Decimal = Decimal(-1)
    pct_chg: Decimal = Decimal(-1)

    def __post_init__(self):
        if type(self) is TickBase:
            self._cant_be_init_error()



