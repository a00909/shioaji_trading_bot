from dataclasses import dataclass
from decimal import Decimal

from shioaji import BidAskFOPv1

from data.unified.bases.bid_ask_base import BidAskBase


@dataclass(frozen=True, slots=True)
class BidAskFOP(BidAskBase):
    bid_total_vol: int = -1
    ask_total_vol: int = -1
    first_derived_bid_price: Decimal = Decimal(-1)
    first_derived_ask_price: Decimal = Decimal(-1)
    first_derived_bid_vol: int = -1
    first_derived_ask_vol: int = -1
    underlying_price: Decimal = Decimal(-1)

    @property
    def _corresponding_sj_type(self):
        return BidAskFOPv1
