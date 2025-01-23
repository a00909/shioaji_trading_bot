import logging
from datetime import timedelta

from line_profiler_pycharm import profile
from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider
from tools.ui_signal_emitter import ui_signal_emitter

logger = logging.getLogger('Ma Strategy')


class MaStrategy(AbsStrategy):

    def __init__(self, ip: IndicatorProvider, len_long: timedelta, len_short: timedelta, unit):
        super().__init__(ip)
        self.len_long = len_long
        self.len_short = len_short
        self.unit = unit
        self.er: EntryReport | None = None

    @staticmethod
    def _msg_template(is_in: bool, is_long: bool, ma, sd, price, slope):
        return (
            f'[{"In" if is_in else "Out"} signal {"long" if is_long else "short"}] '
            f'ma:{ma} | sd:{sd} | price: {price} | slope: {slope}'
        )

    def _indicators(self):
        ma = self.ip.ma(self.len_long)
        price = self.ip.latest_price()
        slope = self.ip.slope(self.len_short, self.len_long)
        sd = self.ip.standard_deviation(self.len_long, self.unit)
        return ma, price, slope, sd

    def _get_report(self, params, ma, sd, price, slope):
        if not params:
            return None

        res = StrategySuggestion()
        res.action = params[0]
        res.quantity = 1
        res.valid = True

        msg = self._msg_template(True, res.action == Action.Buy, ma, sd, price, slope)
        logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)
        return res

    def in_signal(self) -> StrategySuggestion | None:
        ma, price, slope, sd = self._indicators()

        params = None

        if price < ma - sd and slope > 0:
            params = [
                Action.Buy,
            ]
        elif price > ma + sd and slope < 0:
            params = [
                Action.Sell,
            ]
        else:
            pass

        res = self._get_report(params, ma, sd, price, slope)

        return res

    def report_entry(self, er: EntryReport):
        self.er = er
        msg = (
            f'Entry report: deal_time={er.deal_time} '
            f'| cover_price={er.deal_price} '
            f'| direction={er.action} '
            f'| qty={er.quantity} '
        )
        logger.info(msg)
        ui_signal_emitter.emit_strategy(msg)

    @profile
    def out_signal(self) -> StrategySuggestion | None:
        if not self.er:
            logger.info('no entry report yet.')
            return None

        ma, price, slope, sd = self._indicators()

        params = None

        if self.er.action == Action.Buy:
            if price > ma + sd and slope < 0:
                params = [
                    Action.Sell
                ]
        elif self.er.action == Action.Sell:
            if price < ma - sd and slope > 0:
                params = [
                    Action.Buy
                ]

        res = self._get_report(params, ma, sd, price, slope)

        return res

    def name(self):
        return 'ma reversion strategy'
