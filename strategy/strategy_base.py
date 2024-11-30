from abc import ABC, abstractmethod

from realtime.order_placer import OrderPlacer
from tick_manager.realtime_tick_manager import RealtimeTickManager
from tick_manager.history_tick_manager import HistoryTickManager


class StrategyBase(ABC):
    def __init__(self, rtm, htm, op):
        self.realtime_tick_manager: RealtimeTickManager = rtm
        self.history_tick_manager: HistoryTickManager = htm
        self.order_placer: OrderPlacer = op

    @abstractmethod
    def strategy_loop(self):
        pass
