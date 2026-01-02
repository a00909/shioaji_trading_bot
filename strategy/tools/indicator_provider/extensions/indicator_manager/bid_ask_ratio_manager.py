from itertools import takewhile

from redis.client import Redis

from data.unified.bid_ask.bid_ask_fop import BidAskFOP
from strategy.tools.indicator_provider.extensions.data.bid_ask_ratio import BidAskRatio
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager


class BidAskRatioManager(AbsIndicatorManager):
    def __init__(self, length, symbol: str, start_time, redis: Redis, rtm):
        super().__init__(IndicatorType.BID_ASK_RATIO, length, symbol, start_time, redis, rtm)
        self.end_count = None
        self.end_datetime = None

    def calculate(self, now, last: BidAskRatio):
        if not self.rtm.bid_ask_buffer:
            print('no bid_asks!')
            return None

        new = BidAskRatio()
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

        self._collect_end_count(bidasks)
        return self._deal(bidasks)

    def _calc_incr(self, last: BidAskRatio, now):
        # 增量更新

        added_bidasks = self.rtm.get_bidask_by_time_range(last.datetime, now)

        a_bid, a_ask = self._deal_added_ticks(added_bidasks)
        r_bid, r_ask = self._deal_removed_ticks(now, last)

        bid = last.bid + a_bid - r_bid
        ask = last.ask + a_ask - r_ask

        self._collect_end_count(added_bidasks)

        return bid, ask

    def _deal_added_ticks(self, added_ticks: list[BidAskFOP]):
        left = 0
        for e, ba in enumerate(added_ticks):
            if not (ba.datetime <= self.end_datetime and e < self.end_count):
                left = e
                break

        return self._deal(added_ticks[left:])

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

        return self._deal(deprecated)

    @staticmethod
    def _deal(bidasks: list[BidAskFOP]):
        bid = 0
        ask = 0

        for ba in bidasks:
            bid += ba.bid_volume
            ask += ba.ask_volume

        return bid, ask

    def _collect_end_count(self, bidasks: list[BidAskFOP]):
        if not bidasks:
            self.end_count = 0
            return

        last_dt = bidasks[-1].datetime
        reversed_ = reversed(bidasks)

        end_count = sum(1 for _ in takewhile(lambda tick: tick.datetime >= last_dt, reversed_))

        self.end_count = end_count
        self.end_datetime = last_dt
