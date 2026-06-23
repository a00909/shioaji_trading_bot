from abc import ABC
from datetime import date
from functools import cache, lru_cache
from typing import ClassVar

from psycopg import Connection

from data_manager.history._utils import range_check
from data_manager.history.statics.base._field_base import _FieldBase
from data_manager.history.statics.base._history_data_spec import _HistoryDataSpec
from data_manager.history.statics.base._np_data_base import _NpDataBase
from data_manager.history.statics.data import DailySlice
from qclaw.backtesting.npy_cache import CacheState
from qclaw.backtesting.npy_cache.npy_cache_manager import npy_cache_manager
from tools.date_range_utils import enumerate_dates_set_by_range


class _NpyCachedDataManagerBase[D:_NpDataBase](ABC):
    _cache_key_infix: ClassVar[str]

    def __init__(self, data_spec: _HistoryDataSpec, data_manager):
        self._data_spec = data_spec
        self._data_manager = data_manager
        self._npy_cache = npy_cache_manager

    @classmethod
    @cache
    def _key(cls, symbol: str, dt: date, field: _FieldBase) -> str:
        """Generate cache key: symbol.tick.date.field"""
        dt_str = dt.strftime("%Y-%m-%d")
        return f"{symbol}.{cls._cache_key_infix}.{dt_str}.{field}"  # StrEnum auto-converts to str

    def _load_from_npy(self, symbol: str, dt: date) -> tuple[CacheState, D]:
        """Load TickSlice from npy cache. Returns None if not cached."""
        time_key = self._key(symbol, dt, self._data_spec.field_enum.TS)
        cache_state = self._npy_cache.is_cached(time_key)

        if cache_state == CacheState.HIT:
            return cache_state, self._data_spec.np_data_type(
                **{f: self._npy_cache.get(self._key(symbol, dt, f)) for f in self._data_spec.field_enum}
            )
        return cache_state, None

    def _save_to_npy(self, symbol: str, dt: date, slice_: D) -> None:
        """Save TickSlice to npy cache (one file per field per day)."""
        raw = slice_.__dict__
        for f in self._data_spec.field_enum.names():
            self._npy_cache.set(self._key(symbol, dt, f), raw[f])

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

        date_to_npy_slice: dict[date, D] = {}

        missed_dates = set()
        for dt in dates:
            # Try npy cache first
            cache_state, np_data = self._load_from_npy(symbol, dt)

            if cache_state == CacheState.MISS:
                missed_dates.add(dt)
            else:
                date_to_npy_slice[dt] = np_data

        results = []

        if missed_dates:
            date_to_history_ticks: dict[date, D] = self._data_manager.get_ticks(conn, contract, dates=missed_dates)
        else:
            date_to_history_ticks = []

        for dt in sorted(list(dates)):  # 需依照順序
            if dt in date_to_npy_slice:
                if date_to_npy_slice[dt]:
                    results.append(DailySlice[D](dt, date_to_npy_slice[dt]))
            elif dt in date_to_history_ticks:
                np_data = date_to_history_ticks[dt]
                if np_data:
                    results.append(DailySlice[D](dt, np_data))
                    self._save_to_npy(symbol, dt, np_data)
                else:
                    self._npy_cache.mark_empty(self._key(symbol, dt, self._data_spec.field_enum.TS))
            else:
                raise Exception(f"neither in npy cache nor {self._data_spec.table_name} with date {dt}.")

        if not results:
            return []
        return results
