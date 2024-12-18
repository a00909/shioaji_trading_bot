import datetime
import logging

from line_profiler_pycharm import profile
from shioaji.constant import Action

from strategy.strategies.abs_strategy import AbsStrategy
from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider import IndicatorProvider

logger = logging.getLogger('Ma Strategy')


class MaStrategy(AbsStrategy):

    def __init__(self, ip: IndicatorProvider, length: datetime.timedelta):
        super().__init__(ip)
        self.length = length
        self.er: EntryReport | None = None

    def in_signal(self) -> StrategySuggestion | None:
        ma = self.ip.ma(self.length)
        price = self.ip.latest_price()
        is_inc, slope = self.ip.is_increasing(self.length)

        res = None
        if price < ma and is_inc:
            res = StrategySuggestion()
            res.action = Action.Buy
            res.quantity = 1
            res.valid = True
        elif price > ma and not is_inc:
            res = StrategySuggestion()
            res.action = Action.Sell
            res.quantity = 1
            res.valid = True
        else:
            # res.valid = False
            pass
        return res

    def report_entry(self, er: EntryReport):
        self.er = er
        logger.info(
            f'Entry report: deal_time={er.deal_time} '
            f'| cover_price={er.deal_price} '
            f'| direction={er.action} '
            f'| qty={er.quantity} '
        )

    @profile
    def out_signal(self) -> StrategySuggestion | None:
        if not self.er:
            logger.info('no entry report yet.')
            return None
        ma = self.ip.ma(self.length)
        price = self.ip.latest_price()
        is_inc, slope = self.ip.is_increasing(self.length)

        res = None
        if self.er.action == Action.Buy:
            if price > ma and not is_inc:
                res = StrategySuggestion()
                res.action = Action.Sell
                res.quantity = self.er.quantity
                res.valid = True


        elif self.er.action == Action.Sell:
            if price < ma and is_inc:
                res = StrategySuggestion()
                res.action = Action.Buy
                res.quantity = self.er.quantity
                res.valid = True

        return res

    def name(self):
        return 'ma reversion strategy'
