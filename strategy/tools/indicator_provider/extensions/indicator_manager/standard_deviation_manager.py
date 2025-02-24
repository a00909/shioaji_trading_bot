from redis.client import Redis
from typing_extensions import override

from data.tick_fop_v1d1 import TickFOPv1D1
from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.extensions.data.standard_deviation import StandardDeviation
from strategy.tools.indicator_provider.extensions.indicator_manager.abs_indicator_manager import AbsIndicatorManager


class StandardDeviationManager(AbsIndicatorManager):
    """
    buffer actually saves square sums and sample counts
    """

    def __init__(self, length, rtm, symbol, start_time, redis: Redis):
        super().__init__(IndicatorType.SD, length, symbol, start_time, redis, rtm)

        self.end_square_sum = None
        self.end_count = None
        self.end_sum = None

    @override
    def get(self, backward_idx=-1, value_only=True):
        indicator: StandardDeviation = super().get(backward_idx, value_only=False)

        if not indicator:
            return None

        if value_only:
            variance = (
                    (indicator.square_sum / indicator.data_count)
                    - (indicator.sum / indicator.data_count) ** 2
            )
            value = variance ** 0.5
            return value
        else:
            return indicator

    def calculate(self, now, last: StandardDeviation):
        new = StandardDeviation()
        new.datetime = self.rtm.latest_tick().datetime
        new.indicator_type = self.indicator_type
        new.length = self.length

        if last:
            square_sum, summation, count = self._calc_incr(last, now)

            # todo:for test, remove later
            # square_sum2, summation2, count2 = self._calc_first(now)
            # if (count2 != count) or (
            #         square_sum2 > 0 and
            #         deviation(square_sum2, square_sum) > 0.001
            # ) or (
            #         summation2 > 0 and
            #         deviation(summation2, summation) > 0.001
            # ):
            #     raise Exception((
            #         f'{count2},{count};'
            #         f'{summation2},{summation};'
            #         f'{square_sum2},{square_sum};'
            #         f'{len(self.buffer)}'
            #     ))

        else:
            square_sum, summation, count = self._calc_first(now)

        new.square_sum = square_sum
        new.sum = summation
        new.data_count = count
        return new

    def _calc_first(self, now) -> tuple[float, float, int]:

        ticks = self.rtm.get_ticks_by_time_range(now - self.length, now)
        if not ticks:
            raise Exception("No data to calculate!")

        # pma = self.pma_manager.get()

        square_sum = 0
        summation = 0
        count = 0
        for i in ticks:
            square_sum += i.close ** 2 * i.volume
            summation += i.close * i.volume
            count += i.volume

        self._collect_end_data(ticks)

        return square_sum, summation, count

    def _calc_incr(self, last: StandardDeviation, now) -> tuple[float, float, int]:
        added_ticks = self.rtm.get_ticks_by_time_range(last.datetime, now)
        added_square_sum, added_sum, added_count = self._deal_added_ticks(added_ticks)

        deprecated_square_sum, deprecated_sum, deprecated_count = self._deal_removed_ticks(last, now)

        new_sum = (
                last.sum
                + added_sum
                - self.end_sum
                - (deprecated_sum if deprecated_sum else 0)
        )
        new_square_sum = (
                last.square_sum
                + added_square_sum
                - self.end_square_sum
                - (deprecated_square_sum if deprecated_square_sum else 0)
        )
        new_count = (
                last.data_count
                + added_count
                - self.end_count
                - (deprecated_count if deprecated_count else 0)
        )

        self._collect_end_data(added_ticks)

        return new_square_sum, new_sum, new_count

    @staticmethod
    def _deal_added_ticks(added_ticks):
        added_count = sum(t.volume for t in added_ticks)
        added_sum = sum(t.close * t.volume for t in added_ticks)
        added_square_sum = sum(t.close ** 2 * t.volume for t in added_ticks)

        return added_square_sum, added_sum, added_count

    def _deal_removed_ticks(self, last, now):
        if now == last.datetime:
            pass
        elif now < last.datetime:
            raise Exception('now < last.datetime!')
        else:
            deprecated_ticks = self.rtm.get_ticks_by_time_range(
                last.datetime - self.length,
                now - self.length,
                with_end=False
            )
            if deprecated_ticks:
                deprecated_count = sum(t.volume for t in deprecated_ticks)
                deprecated_sum = sum(t.close * t.volume for t in deprecated_ticks)
                deprecated_square_sum = sum(t.close ** 2 * t.volume for t in deprecated_ticks)

                return deprecated_square_sum, deprecated_sum, deprecated_count

        return None, None, None

    def _collect_end_data(self, ticks: list[TickFOPv1D1]):
        last_dt = ticks[-1].datetime
        p = len(ticks) - 1
        self.end_sum = 0
        self.end_count = 0
        self.end_square_sum = 0

        while ticks[p].datetime >= last_dt and p >= 0:
            self.end_sum += ticks[p].close * ticks[p].volume
            self.end_count += ticks[p].volume
            self.end_square_sum += ticks[p].close ** 2 * ticks[p].volume
            p -= 1
