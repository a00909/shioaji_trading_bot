from datetime import date
from functools import lru_cache

from psycopg import Connection
from shioaji import Shioaji

from data_manager.history._utils import range_check
from data_manager.history.htm2 import HistoryTickManager2
from data_manager.history.statics.data import DailySlice
from data_manager.history.statics.np_ticks import NPTicks
from data_manager.history.statics.tick_field import TickField, TICKS_FIELDS
from qclaw.backtesting.npy_cache import CacheState, NpyCacheManager
from tools.date_range_utils import enumerate_dates_set_by_range


class NpyCachedHistoryTickManager:
    def __init__(self, api: Shioaji):
        self.npy_cache = NpyCacheManager()
        self.htm = HistoryTickManager2(api)

    @staticmethod
    @lru_cache(maxsize=None)
    def _key(symbol: str, dt: date, field: TickField) -> str:
        """Generate cache key: symbol.tick.date.field"""
        dt_str = dt.strftime("%Y-%m-%d")
        return f"{symbol}.tick.{dt_str}.{field}"  # StrEnum auto-converts to str

    def _load_from_npy(self, symbol: str, dt: date) -> tuple[CacheState, NPTicks]:
        """Load TickSlice from npy cache. Returns None if not cached."""
        time_key = self._key(symbol, dt, TickField.TS)
        cache_state = self.npy_cache.is_cached(time_key)

        if cache_state == CacheState.HIT:
            return cache_state, NPTicks(
                **{f: self.npy_cache.get(self._key(symbol, dt, f)) for f in TickField}
            )
        return cache_state, None

    def _save_to_npy(self, symbol: str, dt: date, slice_: NPTicks) -> None:
        """Save TickSlice to npy cache (one file per field per day)."""
        raw = slice_.__dict__
        for f in TICKS_FIELDS:
            self.npy_cache.set(self._key(symbol, dt, f), raw[f])

    def get(self, conn: Connection, contract, start, end) -> list[DailySlice]:
        """Get tick data for date range.

        Parameters
        ----------
        contract : Contract
            shioaji Contract object (must have .symbol attribute)
        start : date
            Start date
        end : date
            End date (inclusive)

        Returns
        -------
        'TickSlice'
            TickSlice objects sorted by date.
            Skips dates with empty cache markers.
        """
        range_check(start, end)
        symbol = contract.symbol
        dates = enumerate_dates_set_by_range(start, end)

        date_to_npy_slice: dict[date, NPTicks] = {}

        missed_dates = set()
        for dt in dates:
            # Try npy cache first
            cache_state, np_ticks = self._load_from_npy(symbol, dt)

            if cache_state == CacheState.MISS:
                missed_dates.add(dt)
            else:
                date_to_npy_slice[dt] = np_ticks

        results = []

        if missed_dates:
            date_to_history_ticks: dict[date, NPTicks] = self.htm.get_ticks(conn, contract, dates=missed_dates)
        else:
            date_to_history_ticks = []

        for dt in sorted(list(dates)):  # 需依照順序
            if dt in date_to_npy_slice:
                if date_to_npy_slice[dt]:
                    results.append(DailySlice(dt, date_to_npy_slice[dt]))
            elif dt in date_to_history_ticks:
                np_ticks = date_to_history_ticks[dt]
                if np_ticks:
                    results.append(DailySlice(dt, np_ticks))
                    self._save_to_npy(symbol, dt, np_ticks)
                else:
                    self.npy_cache.mark_empty(self._key(symbol, dt, TickField.TS))
            else:
                raise Exception(f"neither in npy cache nor history ticks with date {dt}.")

        if not results:
            return []
        return results
