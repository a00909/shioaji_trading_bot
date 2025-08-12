import logging
from abc import ABC, abstractmethod
from datetime import time

from shioaji.constant import Action

from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_facade import IndicatorFacade
from tools.ui_signal_emitter import ui_signal_emitter


class AbsStrategy(ABC):

    def __init__(
            self,
            indicator_facade: IndicatorFacade,
            active_time_ranges: list[tuple[time, time]]

    ):
        self.indicator_facade = indicator_facade
        self.active_time_ranges: list[tuple[time, time]] = active_time_ranges

        self.er = None
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
    def _ma_long(self):
        return self.indicator_facade.pma_long()

    @property
    def _ma_short(self):
        return self.indicator_facade.pma_short()

    @property
    def _price(self):
        return self.indicator_facade.latest_price()

    @property
    def _sd(self):
        return self.indicator_facade.sd()

    @property
    def _vma_long(self):
        return self.indicator_facade.vma_long()

    @property
    def _vma_short(self):
        return self.indicator_facade.vma_short()

    @property
    def _covariance_long(self):
        return self.indicator_facade.covariance_long()

    @property
    def _covariance_short(self):
        return self.indicator_facade.covariance_short()

    @property
    def _is_high_volume(self):
        return self._vma_short >= self._vma_long

    @property
    def _sell_buy_ratio(self):
        return self.indicator_facade.sell_buy_diff()

    @property
    def _bid_ask_ratio(self):
        return self.indicator_facade.bid_ask_diff_ma()

    @property
    def _sd_stop_loss(self):
        return self.indicator_facade.sd_stop_loss()

    @property
    def _is_active_time(self):
        current_time = self.indicator_facade.now().time()
        for start, end in self.active_time_ranges:
            if (
                    (end < start and (current_time >= start or current_time <= end)) or
                    (end > start and (start <= current_time <= end))
            ):
                return True

        return False

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

        self.enter_ma = self._ma_long
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
        msg = self._msg_template(is_in, is_long, self._ma_long, self._sd, self._price, self._covariance_long)
        self.logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)
        return res
