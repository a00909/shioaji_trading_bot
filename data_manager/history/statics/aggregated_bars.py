from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from data_manager.history.statics.tick.np_ticks import NPTicks
from tools.time_utils import PG_EPOCH_OFFSET_S


@dataclass
class AggregatedBars:
    """
    將原始 tick 聚合為固定時間間隔的 bar（類似 1 秒 K）。

    所有 numpy array 長度相同，代表每根 bar 的聚合統計量。
    """

    # ── 時間 ───────────────────────────────────────────
    ts: np.ndarray          # float64, 聚合區間起始的 pg epoch (μs)

    # ── OHLCV（基於 close 欄位） ──────────────────────
    open: np.ndarray        # float64, 區間第一筆 close
    high: np.ndarray        # float64, 區間最高 close
    low: np.ndarray         # float64, 區間最低 close
    close: np.ndarray       # float64, 區間最後一筆 close
    volume: np.ndarray      # int64, 區間總成交量
    tick_count: np.ndarray # int64, 區間原始 tick 筆數

    # ── 加權與 order book 統計 ────────────────────────
    vwap: np.ndarray        # float64, volume-weighted avg close
    avg_bid_price: np.ndarray   # float64
    avg_ask_price: np.ndarray   # float64
    avg_bid_volume: np.ndarray  # float64 (int → float 因可能為空)
    avg_ask_volume: np.ndarray  # float64

    # ── 成交方向 ──────────────────────────────────────
    active_buy_volume: np.ndarray   # float64, 外盤成交總量 (tick_type==1)
    active_sell_volume: np.ndarray  # float64, 內盤成交總量 (tick_type==2)

    # ── 推導 ──────────────────────────────────────────
    @property
    def spread(self) -> np.ndarray:
        return self.avg_ask_price - self.avg_bid_price

    @property
    def mid_price(self) -> np.ndarray:
        return (self.avg_ask_price + self.avg_bid_price) / 2

    # ── 便捷方法 ──────────────────────────────────────
    def ts_seconds(self) -> np.ndarray:
        return self.ts / 1_000_000 + PG_EPOCH_OFFSET_S

    def __len__(self) -> int:
        return len(self.ts)

    # ── 工廠方法 ──────────────────────────────────────
    @classmethod
    def from_npticks(
        cls,
        npticks: NPTicks,
        interval_seconds: int = 1,
    ) -> AggregatedBars:
        """
        從 NPTicks 聚合為固定間隔的 bar。

        Parameters
        ----------
        npticks : NPTicks
            原始 tick 資料（一天的）。
        interval_seconds : int
            聚合間隔（秒），預設 1 秒。

        Returns
        -------
        AggregatedBars
        """
        ts_s = npticks.ts_seconds()
        bucket = (ts_s // interval_seconds).astype(np.int64)

        unique_buckets, inverse, counts = np.unique(
            bucket, return_inverse=True, return_counts=True,
        )
        n = len(unique_buckets)

        # OHLCV
        open_ = _agg_first(inverse, n, npticks.close)
        high_ = _agg_max(inverse, n, npticks.close)
        low_ = _agg_min(inverse, n, npticks.close)
        close_ = _agg_last(inverse, n, npticks.close)
        vol_ = _agg_sum(inverse, n, npticks.volume.astype(np.int64))

        # VWAP
        vwap_ = _agg_vwap(inverse, n, npticks.close, npticks.volume.astype(np.float64))

        # Order book 均值
        avg_bid_p = _agg_mean(inverse, n, npticks.bid_price)
        avg_ask_p = _agg_mean(inverse, n, npticks.ask_price)
        avg_bid_v = _agg_mean(inverse, n, npticks.bid_volume.astype(np.float64))
        avg_ask_v = _agg_mean(inverse, n, npticks.ask_volume.astype(np.float64))

        # 成交方向（tick_type: 1=外盤/買方主動, 2=內盤/賣方主動）
        tick_type = npticks.tick_type
        buy_vol = np.where(tick_type == 1, npticks.volume.astype(np.float64), 0.0)
        sell_vol = np.where(tick_type == 2, npticks.volume.astype(np.float64), 0.0)
        active_buy = _agg_sum(inverse, n, buy_vol)
        active_sell = _agg_sum(inverse, n, sell_vol)

        # 時間戳：每個 bucket 起始轉回 pg epoch μs
        bar_ts = (unique_buckets.astype(np.float64) * interval_seconds
                  - PG_EPOCH_OFFSET_S) * 1_000_000

        return cls(
            ts=bar_ts,
            open=open_,
            high=high_,
            low=low_,
            close=close_,
            volume=vol_,
            tick_count=counts.astype(np.int64),
            vwap=vwap_,
            avg_bid_price=avg_bid_p,
            avg_ask_price=avg_ask_p,
            avg_bid_volume=avg_bid_v,
            avg_ask_volume=avg_ask_v,
            active_buy_volume=active_buy,
            active_sell_volume=active_sell,
        )


# ── 聚合原語（純 numpy，無 Python loop）────────────────

def _agg_first(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    """每個 group 中 index 最小的元素的值。"""
    first_idx = np.full(n, len(arr), dtype=np.intp)
    np.minimum.at(first_idx, inverse, np.arange(len(arr), dtype=np.intp))
    return arr[first_idx]


def _agg_last(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    """每個 group 中 index 最大的元素的值。"""
    last_idx = np.full(n, -1, dtype=np.intp)
    np.maximum.at(last_idx, inverse, np.arange(len(arr), dtype=np.intp))
    return arr[last_idx]


def _agg_min(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    out = np.full(n, np.inf, dtype=arr.dtype)
    np.minimum.at(out, inverse, arr)
    return out


def _agg_max(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    out = np.full(n, -np.inf, dtype=arr.dtype)
    np.maximum.at(out, inverse, arr)
    return out


def _agg_sum(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    out = np.zeros(n, dtype=arr.dtype)
    np.add.at(out, inverse, arr)
    return out


def _agg_mean(inverse: np.ndarray, n: int, arr: np.ndarray) -> np.ndarray:
    total = _agg_sum(inverse, n, arr.astype(np.float64))
    counts = _agg_count(inverse, n).astype(np.float64)
    return total / counts


def _agg_vwap(
    inverse: np.ndarray,
    n: int,
    price: np.ndarray,
    volume: np.ndarray,
) -> np.ndarray:
    total_pv = np.zeros(n, dtype=np.float64)
    np.add.at(total_pv, inverse, price * volume)
    total_vol = _agg_sum(inverse, n, volume.astype(np.float64))
    return np.divide(total_pv, total_vol, out=np.zeros(n, dtype=np.float64),
                     where=total_vol != 0)


def _agg_count(inverse: np.ndarray, n: int) -> np.ndarray:
    counts = np.bincount(inverse, minlength=n)
    return counts.astype(np.int64)
