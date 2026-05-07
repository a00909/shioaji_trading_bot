from dataclasses import dataclass
from datetime import date, timedelta
from functools import lru_cache
from typing import Optional

from data_manager.history_data_manager.history_tick_manager import HistoryTickManager, DailyTicks
from database.schema.history_tick import HistoryTick
from qclaw.backtesting.npy_cache import NpyCacheManager

import numpy as np

from tools.date_range_utils import enumerate_dates_set_by_range


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
        """合併多個 TickSlice 物件，日期以最後一個為代表或設為 None"""
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


@dataclass
class DailyTickSlice:
    date: date
    tick_slice: TickSlice


class NpyCachedHistoryTickManager:
    def __init__(self, npy_cache_manager: NpyCacheManager, htm: HistoryTickManager):
        self.npy_cache = npy_cache_manager
        self.htm = htm

    @staticmethod
    @lru_cache(maxsize=None)
    def _key(symbol: str, dt: date, field: str) -> str:
        """Generate cache key: symbol.tick.date.field"""
        dt_str = dt.strftime("%Y-%m-%d")
        return f"{symbol}.tick.{dt_str}.{field}"

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

    def _load_from_npy(self, symbol: str, dt: date) -> Optional[TickSlice]:
        """Load TickSlice from npy cache. Returns None if not cached."""
        # Check first field to see if cache hit/miss
        time_key = self._key(symbol, dt, "times")
        cache_state = self.npy_cache.is_cached(time_key)

        if cache_state == "hit":
            # Load all fields from npy
            times = self.npy_cache.get(self._key(symbol, dt, "times"))
            closes = self.npy_cache.get(self._key(symbol, dt, "closes"))
            volumes = self.npy_cache.get(self._key(symbol, dt, "volumes"))
            tick_types = self.npy_cache.get(self._key(symbol, dt, "tick_types"))
            bid_prices = self.npy_cache.get(self._key(symbol, dt, "bid_prices"))
            ask_prices = self.npy_cache.get(self._key(symbol, dt, "ask_prices"))
            bid_volumes = self.npy_cache.get(self._key(symbol, dt, "bid_volumes"))
            ask_volumes = self.npy_cache.get(self._key(symbol, dt, "ask_volumes"))

            return TickSlice(
                times=times,
                closes=closes,
                volumes=volumes,
                tick_types=tick_types,
                bid_prices=bid_prices,
                ask_prices=ask_prices,
                bid_volumes=bid_volumes,
                ask_volumes=ask_volumes,
            )
        elif cache_state == "empty":
            # Confirmed no data for this date
            return None
        else:
            # Cache miss - need to fetch from HTM
            return None

    def _save_to_npy(self, symbol: str, dt: date, slice_: TickSlice) -> None:
        """Save TickSlice to npy cache (one file per field per day)."""
        self.npy_cache.set(self._key(symbol, dt, "times"), slice_.times)
        self.npy_cache.set(self._key(symbol, dt, "closes"), slice_.closes)
        self.npy_cache.set(self._key(symbol, dt, "volumes"), slice_.volumes)
        self.npy_cache.set(self._key(symbol, dt, "tick_types"), slice_.tick_types)
        self.npy_cache.set(self._key(symbol, dt, "bid_prices"), slice_.bid_prices)
        self.npy_cache.set(self._key(symbol, dt, "ask_prices"), slice_.ask_prices)
        self.npy_cache.set(self._key(symbol, dt, "bid_volumes"), slice_.bid_volumes)
        self.npy_cache.set(self._key(symbol, dt, "ask_volumes"), slice_.ask_volumes)

    def get(self, contract, start, end) -> TickSlice:
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
            slice_ = self._load_from_npy(symbol, dt)
            if slice_ is not None:
                date_to_npy_slice[dt] = slice_
                continue

            # Check if confirmed empty
            time_key = self._key(symbol, dt, "times")
            if self.npy_cache.has_empty(time_key):
                date_to_npy_slice[dt] = None
                continue

            missed_dates.append(dt)

        # Fetch from HTM, cache result
        daily_ticks_list: list[DailyTicks] = self.htm.get_data_batch(contract, dates=missed_dates)
        date_to_history_ticks = {daily_ticks.date: daily_ticks.ticks for daily_ticks in daily_ticks_list}

        results = []
        for dt in dates:
            if dt in date_to_npy_slice and date_to_npy_slice[dt]:
                results.append(date_to_npy_slice[dt])
            elif dt in date_to_history_ticks:
                ticks = date_to_history_ticks[dt]
                if ticks:
                    slice_ = self._build_tick_slice(ticks)
                    results.append(slice_)
                    self._save_to_npy(symbol, dt, slice_)

                else:
                    self.npy_cache.mark_empty(self._key(symbol, dt, "times"))
            else:
                raise Exception(f"neither in npy cache nor history ticks with date {dt}.")

        return TickSlice.merge_slices(results)
