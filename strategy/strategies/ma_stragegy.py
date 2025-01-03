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

    def __init__(self, ip: IndicatorProvider, len_long: timedelta, len_short: timedelta):
        super().__init__(ip)
        self.len_long = len_long
        self.len_short = len_short
        self.er: EntryReport | None = None

    def in_signal(self) -> StrategySuggestion | None:
        ma = self.ip.ma(self.len_long)
        price = self.ip.latest_price()
        slope = self.ip.slope(self.len_long, self.len_short)

        res = None
        if price < ma and slope > 0:
            res = StrategySuggestion()
            res.action = Action.Buy
            res.quantity = 1
            res.valid = True
            msg = (
                f'[In signal long] ma:{ma} | price: {price} | slope: {slope}'
            )
            logger.info(msg)
            ui_signal_emitter.emit_strategy(msg)

        elif price > ma and slope < 0:
            res = StrategySuggestion()
            res.action = Action.Sell
            res.quantity = 1
            res.valid = True
            msg = (
                f'[In signal short] ma:{ma} | price: {price} | slope: {slope}'
            )
            logger.info(msg)
            ui_signal_emitter.emit_strategy(msg)
        else:
            # res.valid = False
            pass
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
        ma = self.ip.ma(self.len_long)
        price = self.ip.latest_price()
        slope = self.ip.slope(self.len_long, self.len_short)

        res = None
        if self.er.action == Action.Buy:
            if price > ma and slope < 0:
                res = StrategySuggestion()
                res.action = Action.Sell
                res.quantity = self.er.quantity
                res.valid = True
                msg = (
                    f'[Out signal long] ma:{ma} | price: {price} | slope: {slope}'
                )
                logger.info(msg)
                ui_signal_emitter.emit_strategy(msg)


        elif self.er.action == Action.Sell:
            if price < ma and slope > 0:
                res = StrategySuggestion()
                res.action = Action.Buy
                res.quantity = self.er.quantity
                res.valid = True
                msg = (
                    f'[Out signal short] ma:{ma} | price: {price} | slope: {slope}'
                )
                logger.info(msg)
                ui_signal_emitter.emit_strategy(msg)

        return res

    def name(self):
        return 'ma reversion strategy'
