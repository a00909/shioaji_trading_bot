from itertools import takewhile

from redis.client import Redis
from typing_extensions import override

from data.bid_ask_fop_v1d1 import BidAskFOPv1D1
from strategy.tools.indicator_provider.extensions.data.bid_ask_diff import BidAsk
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager


class BidAskDiffManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.BID_ASK_DIFF, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None

    @override
    def get(self, backward_idx=-1, value_only=True):
        indicator: BidAsk = super().get(backward_idx, value_only=False)

        if not indicator:
            return None

        if value_only:
            u = (indicator.bid + indicator.ask)

            return indicator.bid / u if u else 0
        else:
            return indicator

    def calculate(self, now, last: BidAsk):
        if not self.rtm.bid_ask_buffer:
            print('no bid_asks!')
            return None

        new = BidAsk()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            # 增量更新
            bid, ask = self._calc_incr(last, now)

        else:
            bid, ask = self._calc_first(now)

        new.bid = bid
        new.ask = ask

        return new

    def _calc_first(self, now):
        bidasks = self.rtm.get_bidask_by_time_range(now - self.length, now)
        if len(bidasks) == 0:
            print(
                f'no data to calculate! query range: ({now - self.length},{now}), buffer size: {len(self.rtm.bid_ask_buffer)}')
            return

        bid = 0
        ask = 0

        for ba in bidasks:
            bid += ba.bid_volume
            ask += ba.ask_volume

        self._collect_end_count(bidasks)
        return bid, ask

    def _calc_incr(self, last: BidAsk, now):
        # 增量更新

        added_bidasks = self.rtm.get_bidask_by_time_range(last.datetime, now)

        a_bid, a_ask = self._deal_added_ticks(added_bidasks)
        r_bid, r_ask = self._deal_removed_ticks(now, last)

        bid = last.bid + a_bid - r_bid
        ask = last.ask + a_ask - r_ask

        self._collect_end_count(added_bidasks)

        return bid, ask

    def _deal_added_ticks(self, added_ticks: list[BidAskFOPv1D1]):

        bid = 0
        ask = 0

        for e, ba in enumerate(added_ticks):
            if ba.datetime <= self.end_datetime and e < self.end_count:
                continue
            bid += ba.bid_volume
            ask += ba.ask_volume

        return bid, ask

    def _deal_removed_ticks(self, now, last):
        if now == last.datetime:
            deprecated = []
        elif now < last.datetime:
            raise Exception('now < last.datetime!')
        else:
            deprecated = self.rtm.get_bidask_by_time_range(
                last.datetime - self.length,
                now - self.length,
                with_end=False
            )

        bid = 0
        ask = 0

        if deprecated:
            for ba in deprecated:
                bid += ba.bid_volume
                ask += ba.ask_volume

        return bid, ask

    def _collect_end_count(self, bidasks: list[BidAskFOPv1D1]):
        if not bidasks:
            self.end_count = 0
            return

        last_dt = bidasks[-1].datetime
        reversed_ = reversed(bidasks)

        end_count = sum(1 for _ in takewhile(lambda tick: tick.datetime >= last_dt, reversed_))

        self.end_count = end_count
        self.end_datetime = last_dt
