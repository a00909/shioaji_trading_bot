from dataclasses import dataclass

from shioaji import TickSTKv1

from data.unified.bases.tick_base import TickBase


@dataclass(frozen=True, slots=True)
class TickSTK(TickBase):
    bid_side_total_cnt: int = -1
    ask_side_total_cnt: int = -1
    closing_oddlot_shares: int = -1
    fixed_trade_vol: int = -1
    suspend: bool = False
    intraday_odd: bool = False

    @property
    def _corresponding_sj_type(self):
        return TickSTKv1
