from collections.abc import Iterable

from qclaw.backtesting.data.dc_stat_record import DonchianStatRecord
from qclaw.backtesting.dc_backtesting_context import DonchianBacktestingContext

"""
統計 DC累計因子(DCA)超過不同閾值後的價格成長分布
變數:閾值/DC長度/
終止條件:DCA低點變低 或 收盤 ?
"""


class DonchianStat(DonchianBacktestingContext):
    def __init__(self, test_dates, test_thresholds: list[int]):
        super().__init__(test_dates)
        self.thresholds = test_thresholds
        self.records = None
        self.launch_threshold = min(self.thresholds)

    def _long_signal(self, i, threshold):
        return self.has[i] >= threshold and (i == 0 or self.has[i - 1] < threshold)

    def _short_signal(self, i, threshold):
        return self.las[i] >= threshold and (i == 0 or self.las[i - 1] < threshold)

    def _market_closed(self, i):
        if i >= self.n_total:
            return True
        if i < self.n_total - 1 and self.times[i + 1] - self.times[i] > 60 * 60:
            return True
        return False

    def _init_record(self, i, direction, threshold):
        dr = DonchianStatRecord()
        dr.entry_price, dr.entry_time, dr.day = self._get_tick_data(i)
        dr.direction = direction
        dr.threshold = threshold
        dr.max_accum = self.has[i] if direction == 1 else self.las[i]
        dr.peak_price = dr.entry_price
        return dr

    def _update_record(self, records: Iterable[DonchianStatRecord], i):
        for r in records:
            price = self.prices[i]
            peak_price_func = max if r.direction == 1 else min
            dc_accums = self.has if r.direction == 1 else self.las

            r.max_accum = max(r.max_accum, dc_accums[i])
            r.peak_price = peak_price_func(r.peak_price, price)

            # MAE（最大逆流幅度）：做多曾經比進場價低多少，做空曾經比進場價高多少
            # 兩個公式都確保：獲利時mae不增加，只有逆流時才更新（永遠取max，mae>=0）
            if r.direction == 1:
                r.mae = max(r.mae, r.entry_price - price)
            else:
                r.mae = max(r.mae, price - r.entry_price)

    def _final_record(self, records: Iterable[DonchianStatRecord], i):
        for r in records:
            r.exit_price, r.exit_time, _ = self._get_tick_data(i)

    def stat(self):
        i = 0
        records = []

        temp_records_long: dict[float | int, DonchianStatRecord] = {}
        temp_records_short: dict[float | int, DonchianStatRecord] = {}

        def final_record_long():
            if temp_records_long:
                self._update_record(temp_records_long.values(), i)
                self._final_record(temp_records_long.values(), i)
                records.extend(temp_records_long.values())
                temp_records_long.clear()

        def final_record_short():
            if temp_records_short:
                self._update_record(temp_records_short.values(), i)
                self._final_record(temp_records_short.values(), i)
                records.extend(temp_records_short.values())
                temp_records_short.clear()

        while i < self.n_total:
            closed = self._market_closed(i)

            # LONG
            if self.has[i] >= self.launch_threshold:
                for threshold in self.thresholds:
                    if self._long_signal(i, threshold):
                        temp_records_long[threshold] = self._init_record(i, 1, threshold)
                self._update_record(temp_records_long.values(), i)
            elif self.has[i] <= 0 and temp_records_long:
                final_record_long()

            # SHORT
            if self.las[i] >= self.launch_threshold:
                for threshold in self.thresholds:
                    if self._short_signal(i, threshold):
                        temp_records_short[threshold] = self._init_record(i, -1, threshold)
                self._update_record(temp_records_short.values(), i)
            elif self.las[i] <= 0 and temp_records_short:
                final_record_short()

            # 收盤結算
            if closed:
                final_record_long()
                final_record_short()

            i += 1

        # 迴圈結束後 flush（防止遺漏）
        # i -= 1
        # final_record_long()
        # final_record_short()

        return records
