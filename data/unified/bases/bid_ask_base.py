from abc import ABC
from dataclasses import dataclass
from decimal import Decimal

from data.unified.bases.market_data_base import MarketDataBase


@dataclass(frozen=True, slots=True)
class BidAskBase(MarketDataBase, ABC):
    bid_price: list[Decimal] = None
    bid_volume: list[int] = None
    diff_bid_vol: list[int] = None
    ask_price: list[Decimal] = None
    ask_volume: list[int] = None
    diff_ask_vol: list[int] = None

    def __post_init__(self):
        if type(self) is BidAskBase:
            self._cant_be_init_error()
