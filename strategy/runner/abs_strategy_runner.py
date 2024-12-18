import threading
from abc import ABC, abstractmethod

from shioaji import shioaji
from shioaji.constant import Action, OrderState
from shioaji.position import FuturePosition

from strategy.tools.order_placer import OrderPlacer
from strategy.tools.indicator_provider import IndicatorProvider
from tick_manager.history_tick_manager import HistoryTickManager


class AbsStrategyRunner(ABC):

    def __init__(self, rtm, htm, op):
        # self.rtm: RealtimeTickManager = rtm
        self.history_tick_manager: HistoryTickManager = htm
        self.order_placer: OrderPlacer = op



        self.thread = threading.Thread(target=self.strategy_loop)
        self.run = False

        self.positions: list = []
        self.long_positions: list = []
        self.short_positions: list = []
        self.ip = IndicatorProvider(rtm)

    @abstractmethod
    def order_callback(self, state: OrderState, msg: dict):
        pass

    @abstractmethod
    def strategy_loop(self):
        pass

    def safe_join(self):
        try:
            self.thread.join()
            print('strategy runner stopped.')
        except RuntimeError as e:
            if str(e) == "cannot join thread before it is started":
                print("Thread was not started or has been finished.")
            else:
                raise  # 如果是其他例外，重新拋出

    def start(self):
        self.run = True
        self.ip.start()
        self.thread.start()

    def stop(self, close_all=True):
        self.run = False
        self.ip.stop()
        self.safe_join()

        if close_all:
            if self.order_placer.close_all():
                self.order_placer.wait_for_completely_deal()

    def update_positions(self):
        self.positions: list[FuturePosition] = self.order_placer.list_positions()
        self.long_positions.clear()
        self.short_positions.clear()

        for p in self.positions:
            if p.direction == Action.Buy:
                self.long_positions.append(p)
            elif p.direction == Action.Sell:
                self.short_positions.append(p)
