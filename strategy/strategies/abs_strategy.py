import logging
from abc import ABC, abstractmethod
from datetime import time

from shioaji.constant import Action

from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.strategies.extensions.indicator_property_mixin import IndicatorPropertyMixin
from strategy.tools.indicator_provider.extensions.data.donchian import Donchian
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from tools.ui_signal_emitter import ui_signal_emitter
from tools.utils import is_in_time_ranges


class AbsStrategy(IndicatorPropertyMixin,ABC):

    def __init__(self, indicator_facade: IndicatorFacade, active_time_ranges: list[tuple[time, time]]):
        super().__init__(indicator_facade)
        self.indicator_facade = indicator_facade
        self.active_time_ranges: list[tuple[time, time]] = active_time_ranges

        self.er: EntryReport = None
        self.stop_loss = None
        self.take_profit = None
        self.enter_ma = None
        self.enter_sd = None

        self.logger = logging.getLogger(self.name)

    @property
    @abstractmethod
    def name(self):
        raise NotImplemented

    @abstractmethod
    def in_signal(self) -> StrategySuggestion | None:
        pass

    @abstractmethod
    def _report_entry_detail(self, er: EntryReport):
        pass

    @abstractmethod
    def out_signal(self) -> StrategySuggestion | None:
        pass

    @property
    def _is_active_time(self):
        return is_in_time_ranges(
            self.indicator_facade.now().time(),
            self.active_time_ranges
        )



    def report_entry(self, er: EntryReport):
        self.er = er
        msg = (
            f'Entry report: deal_time={er.deal_time} '
            f'| cover_price={er.deal_price} '
            f'| direction={er.action} '
            f'| qty={er.quantity} '
        )
        self.logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)

        self.stop_loss = None
        self.take_profit = None

        self.enter_ma = self._ma_l
        self.enter_sd = self._sd

        self._report_entry_detail(er)

    @staticmethod
    def _msg_template(is_in: bool, is_long: bool, ma, sd, price, covariance):
        return (
            f'[{"In" if is_in else "Out"} signal {"long" if is_long else "short"}] '
            f'ma:{ma} | sd:{sd} | price: {price} | covariance: {covariance}'
        )

    def _get_report(self, params, is_in=True):
        if not params:
            return None

        res = StrategySuggestion()
        res.action = params[0]
        res.quantity = 1
        res.valid = True

        is_buy = res.action == Action.Buy
        is_long = is_buy == is_in
        msg = self._msg_template(is_in, is_long, self._ma_l, self._sd, self._price, self._covariance_long)
        self.logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)
        return res
