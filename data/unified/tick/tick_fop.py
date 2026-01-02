from dataclasses import dataclass
from decimal import Decimal

from shioaji import TickFOPv1

from data.unified.bases.tick_base import TickBase


@dataclass(frozen=True, slots=True)
class TickFOP(TickBase):
    underlying_price: Decimal = Decimal(-1)

    @property
    def _corresponding_sj_type(self):
        return TickFOPv1
