import logging
from abc import ABC, abstractmethod
from datetime import time, timedelta

from shioaji.constant import Action

from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter


class AbsStrategy(ABC):

    def __init__(
            self,
            ip: IndicatorProvider,
            len_long,
            len_short,
            len_covariance,
            len_vma_short,
            long_vma_times,
            unit,
            active_time_ranges: list[tuple[time, time]]

    ):
        self.ip = ip
        self.len_long = len_long
        self.len_short = len_short
        self.len_covariance = len_covariance
        self.len_vma_short = len_vma_short
        self.long_vma_times = long_vma_times
        self.unit = unit

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
    def _ma(self):
        return self.ip.ma(self.len_long)

    @property
    def _price(self):
        return self.ip.latest_price()

    @property
    def _slope(self):
        return self.ip.slope(self.len_short, self.len_long)

    @property
    def _sd(self):
        return self.ip.standard_deviation(self.len_long)

    @property
    def _vma_long(self):
        return self.ip.vma(self.len_long, self.unit)

    @property
    def _vma_short(self):
        return self.ip.vma(self.len_vma_short, self.unit)

    @property
    def _covariance(self):
        return self.ip.covariance(self.len_covariance)

    @property
    def _is_high_volume(self):
        return self._vma_short >= self._vma_long * self.long_vma_times

    @property
    def _is_active_time(self):
        current_time = self.ip.now.time()
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

        self.enter_ma = self._ma
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
        msg = self._msg_template(is_in, is_long, self._ma, self._sd, self._price, self._covariance)
        self.logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)
        return res
