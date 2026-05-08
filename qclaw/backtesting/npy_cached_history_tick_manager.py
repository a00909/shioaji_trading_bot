from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from functools import lru_cache
from typing import Optional

from data_manager.history_data_manager.history_tick_manager import HistoryTickManager, DailyTicks
from database.schema.history_tick import HistoryTick
from qclaw.backtesting.npy_cache import CacheState, NpyCacheManager

import numpy as np

from tools.date_range_utils import enumerate_dates_set_by_range


class TickField(StrEnum):
    """Npy cache field names — single source of truth."""
    TIMES = "times"
    CLOSES = "closes"
    VOLUMES = "volumes"
    TICK_TYPES = "tick_types"
    BID_PRICES = "bid_prices"
    ASK_PRICES = "ask_prices"
    BID_VOLUMES = "bid_volumes"
    ASK_VOLUMES = "ask_volumes"

    @classmethod
    def from_dataclass(cls, slice_: 'TickSlice') -> dict['TickField', np.ndarray]:
        """Return field→array dict from a TickSlice, matching enum order."""
        return {f: getattr(slice_, f) for f in cls}


@dataclass
class TickSlice:
    """Single day's tick data as numpy arrays."""
    times: np.ndarray  # float64, Unix timestamps
    closes: np.ndarray  # float64
    volumes: np.ndarray  # int64
    tick_types: np.ndarray  # int32
    bid_prices: np.ndarray  # float64
    ask_prices: np.ndarray  # float64
    bid_volumes: np.ndarray  # int64
    ask_volumes: np.ndarray  # int64

    @staticmethod
    def merge_slices(slices: list['TickSlice']) -> 'TickSlice':
        if not slices:
            raise ValueError("The slice list is empty.")

        return TickSlice(  # 通常合併後日期可能跨天，這裡取末尾或自定義
            times=np.concatenate([s.times for s in slices]),
            closes=np.concatenate([s.closes for s in slices]),
            volumes=np.concatenate([s.volumes for s in slices]),
            tick_types=np.concatenate([s.tick_types for s in slices]),
            bid_prices=np.concatenate([s.bid_prices for s in slices]),
            ask_prices=np.concatenate([s.ask_prices for s in slices]),
            bid_volumes=np.concatenate([s.bid_volumes for s in slices]),
            ask_volumes=np.concatenate([s.ask_volumes for s in slices])
        )


class NpyCachedHistoryTickManager:
    def __init__(self, npy_cache_manager: NpyCacheManager, htm: HistoryTickManager):
        self.npy_cache = npy_cache_manager
        self.htm = htm

    @staticmethod
    @lru_cache(maxsize=None)
    def _key(symbol: str, dt: date, field: TickField) -> str:
        """Generate cache key: symbol.tick.date.field"""
        dt_str = dt.strftime("%Y-%m-%d")
        return f"{symbol}.tick.{dt_str}.{field}"  # StrEnum auto-converts to str

    @staticmethod
    def _build_tick_slice(ticks: list[HistoryTick]) -> TickSlice:
        """Build TickSlice from list of HistoryTick."""

        return TickSlice(
            times=np.array([t.ts.timestamp() for t in ticks], dtype=np.float64),
            closes=np.array([float(t.close) for t in ticks], dtype=np.float64),
            volumes=np.array([t.volume for t in ticks], dtype=np.int64),
            tick_types=np.array([t.tick_type for t in ticks], dtype=np.int32),
            bid_prices=np.array(
                [float(t.bid_price) if t.bid_price else 0.0 for t in ticks],
                dtype=np.float64
            ),
            ask_prices=np.array(
                [float(t.ask_price) if t.ask_price else 0.0 for t in ticks],
                dtype=np.float64
            ),
            bid_volumes=np.array(
                [t.bid_volume if t.bid_volume else 0 for t in ticks],
                dtype=np.int64
            ),
            ask_volumes=np.array(
                [t.ask_volume if t.ask_volume else 0 for t in ticks],
                dtype=np.int64
            ),
        )

    def _load_from_npy(self, symbol: str, dt: date) -> tuple[CacheState, Optional[TickSlice]]:
        """Load TickSlice from npy cache. Returns None if not cached."""
        time_key = self._key(symbol, dt, TickField.TIMES)
        cache_state = self.npy_cache.is_cached(time_key)

        if cache_state == CacheState.HIT:
            return cache_state, TickSlice(
                **{f: self.npy_cache.get(self._key(symbol, dt, f)) for f in TickField}
            )
        return cache_state, None

    def _save_to_npy(self, symbol: str, dt: date, slice_: TickSlice) -> None:
        """Save TickSlice to npy cache (one file per field per day)."""
        for f, arr in TickField.from_dataclass(slice_).items():
            self.npy_cache.set(self._key(symbol, dt, f), arr)

    def get(self, contract, start, end) -> TickSlice | None:
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
        TickSlice
            TickSlice objects sorted by date.
            Skips dates with empty cache markers.
        """
        symbol = contract.symbol
        dates = enumerate_dates_set_by_range(start, end)

        date_to_npy_slice: dict[date, TickSlice | None] = {}

        missed_dates = []
        for dt in dates:
            # Try npy cache first
            cache_state, slice_ = self._load_from_npy(symbol, dt)

            if cache_state == CacheState.MISS:
                missed_dates.append(dt)
            else:
                date_to_npy_slice[dt] = slice_

        results = []

        if missed_dates:
            daily_ticks_list: list[DailyTicks] = self.htm.get_data_batch(contract, dates=missed_dates)
            date_to_history_ticks = {d.date: d.ticks for d in daily_ticks_list}
        else:
            date_to_history_ticks = []

        for dt in dates:
            if dt in date_to_npy_slice:
                if date_to_npy_slice[dt]:
                    results.append(date_to_npy_slice[dt])
            elif dt in date_to_history_ticks:
                ticks = date_to_history_ticks[dt]
                if ticks:
                    slice_ = self._build_tick_slice(ticks)
                    results.append(slice_)
                    self._save_to_npy(symbol, dt, slice_)
                else:
                    self.npy_cache.mark_empty(self._key(symbol, dt, TickField.TIMES))
            else:
                raise Exception(f"neither in npy cache nor history ticks with date {dt}.")

        if not results:
            return None
        return TickSlice.merge_slices(results)
