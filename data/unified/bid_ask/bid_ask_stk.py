from dataclasses import dataclass

from shioaji import BidAskSTKv1

from data.unified.bases.bid_ask_base import BidAskBase


@dataclass(frozen=True, slots=True)
class BidAskSTK(BidAskBase):
    suspend: bool = False
    intraday_odd: bool = False

    @property
    def _corresponding_sj_type(self):
        return BidAskSTKv1
