from collections import deque

from shioaji import TickFOPv1, TickSTKv1, BidAskFOPv1, BidAskSTKv1, constant
from shioaji.contracts import Contract
from typing_extensions import Deque

from data.unified.bases.bid_ask_base import BidAskBase
from data.unified.bases.tick_base import TickBase
from data.unified.bid_ask.bid_ask_fop import BidAskFOP
from data.unified.bid_ask.bid_ask_stk import BidAskSTK
from data.unified.tick.tick_fop import TickFOP
from data.unified.tick.tick_stk import TickSTK
from fs_arbitrage.unit_dirty_emitter import UnitDirtyEmitter


class VarietyUnit:
    def __init__(
            self,
            contract: Contract,
            face_value: float,
            fixed_transaction_cost: float,
            proportional_transaction_cost: float,
            buffer_size: int = 100,
            event_queue: UnitDirtyEmitter = None,
    ):
        self._tick_buffer: Deque[TickBase] = deque(maxlen=buffer_size)
        self._bidask_buffer: Deque[BidAskBase] = deque(maxlen=buffer_size)

        self._contract: Contract = contract
        self.face_value = face_value

        self.fixed_transaction_cost = fixed_transaction_cost
        self.proportional_transaction_cost = proportional_transaction_cost

        self._event_emitter = event_queue

    TRANSITION_MAPPING = {
        TickFOPv1: TickFOP,
        TickSTKv1: TickSTK,
        BidAskFOPv1: BidAskFOP,
        BidAskSTKv1: BidAskSTK,
    }


    @property
    def code(self):
        return self._contract.code

    def _dispatch(self, unified_data: TickBase | BidAskBase):
        if isinstance(unified_data, TickBase):
            self._tick_buffer.append(unified_data)
        elif isinstance(unified_data, BidAskBase):
            self._bidask_buffer.append(unified_data)

    def on_sj_data(self, sj_data: TickFOPv1 | TickSTKv1 | BidAskFOPv1 | BidAskSTKv1):
        corresponding_unified_type = self.TRANSITION_MAPPING.get(type(sj_data))
        if not corresponding_unified_type:
            raise TypeError(f'unsupported sj data type: {type(sj_data)}')
        unified_data = corresponding_unified_type.from_sj(sj_data)
        self._dispatch(unified_data)

        if self._event_emitter:
            self._event_emitter(self.code)

    @staticmethod
    def _latest(buffer):
        if len(buffer) > 0:
            return buffer[-1]
        else:
            return None

    def latest_tick(self) -> TickBase | None:
        return self._latest(self._tick_buffer)

    def latest_bidask(self) -> BidAskBase | None:
        return self._latest(self._bidask_buffer)

    def get_tick_sub_data(self):
        return {
            'contract': self._contract,
            'quote_type': constant.QuoteType.Tick,
        }

    def get_bidask_sub_data(self):
        return {
            'contract': self._contract,
            'quote_type': constant.QuoteType.BidAsk,
        }

    @property
    def bid1(self):  # 買1價
        return self.latest_bidask().bid_price[0]

    @property
    def bid1vol(self):
        return self.latest_bidask().bid_volume[0]

    @property
    def ask1(self):
        return self.latest_bidask().ask_price[0]

    @property
    def ask1vol(self):
        return self.latest_bidask().ask_volume[0]
