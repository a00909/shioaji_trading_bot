from dataclasses import dataclass

import numpy as np

from .protocols import MarketContext, SignalResult, StrategyComponent, Intent
from .statistics import BacktestStatistics
from .trade_record import PositionLeg, TradeRecord


@dataclass
class BacktestConfig:
    slippage: float = 1.2
    commission: float = 0.0
    max_lots: int = 5
    flip_allowed: bool = True


class Backtester:
    def __init__(
        self,
        prices: np.ndarray,
        features: np.ndarray | None,
        components: list[StrategyComponent],
        config: BacktestConfig | None = None,
    ):
        self._prices = prices
        self._features = features
        self._components = list(components)
        self._cfg = config or BacktestConfig()

        self._size: int = 0
        self._legs: list[PositionLeg] = []
        self._vwap: float = 0.0
        self._pnl: float = 0.0
        self._entry_idx: int = 0
        self._trades: list[TradeRecord] = []

    def run(self) -> BacktestStatistics:
        p = self._prices
        n = len(p)
        cfg = self._cfg

        for i in range(n):
            price = p[i]

            ctx = MarketContext(
                idx=i, price=price, prices=p, 
                features=self._features,
                current_size=self._size,
                vwap_entry=self._vwap,
                cumulative_pnl=self._pnl,
                holding_bars=i - self._entry_idx if self._size != 0 else 0,
            )

            target = self._size
            reason = ""

            # 兩輪聚合: exit 風控優先, entry 次之
            # exit: 第一個觸發的 component 決定 (風控緊急性)
            # entry: 第一個非 None 的 entry 決定
            exit_target = None
            exit_reason = ""
            entry_target = None
            entry_source = ""
            saw_exit = False
            for comp in self._components:
                r = comp.on_bar(ctx)
                if r is None:
                    continue
                if r.intent == Intent.EXIT:
                    if exit_target is None:
                        exit_target = r.target_size
                        exit_reason = r.source
                    saw_exit = True
                elif r.intent == Intent.ENTRY and entry_target is None:
                    entry_target = r.target_size
                    entry_source = r.source
            if saw_exit:
                # Exit 觸發 → 跳過 entry, 只看 exit
                entry_target = None
                entry_source = ""

            if exit_target is not None:
                target = exit_target
                reason = exit_reason
            elif entry_target is not None:
                target = entry_target
                reason = entry_source
            else:
                continue  # 沒決策, 維持現倉

            if cfg.max_lots > 0:
                target = max(-cfg.max_lots, min(cfg.max_lots, target))
            if target == self._size:
                continue

            if target == 0:
                if self._size != 0:
                    self._finalize(i, price, reason or "signal")
                continue

            if self._size == 0:
                self._add(i, price, abs(target), np.sign(target))
            elif np.sign(target) == np.sign(self._size):
                delta = target - self._size
                if delta > 0:
                    self._add(i, price, delta, np.sign(target))
                else:
                    self._reduce(i, price, abs(delta))
            elif cfg.flip_allowed:
                self._finalize(i, price, f"flip_{reason or 'signal'}" if reason else "flip")
                self._add(i, price, abs(target), np.sign(target))

        if self._size != 0:
            self._finalize(n - 1, p[-1], "end_of_data")

        return BacktestStatistics(trades=self._trades)

    # ── internal ──

    def _add(self, idx: int, price: float, lots: int, side: int):
        if self._size == 0:
            self._entry_idx = idx
            for comp in self._components:
                if hasattr(comp, "reset"):
                    comp.reset()
        ep = price + side * self._cfg.slippage
        self._legs.append(PositionLeg(idx=idx, price=ep, size=lots, reason="signal"))
        total = sum(leg.size for leg in self._legs)
        self._vwap = sum(leg.price * leg.size for leg in self._legs) / total
        self._size = total * side

    def _reduce(self, idx: int, price: float, lots: int):
        lots = min(lots, abs(self._size))
        remaining = lots
        side = 1 if self._size > 0 else -1

        while remaining > 0 and self._legs:
            leg = self._legs[0]
            take = min(leg.size, remaining)
            ex = price - side * self._cfg.slippage
            self._pnl += (ex - leg.price) * side * take - self._cfg.commission * take * 2
            if take >= leg.size:
                self._legs.pop(0)
            else:
                leg.size -= take
            remaining -= take

        total = sum(leg.size for leg in self._legs)
        self._size = total * side if total > 0 else 0
        self._vwap = sum(leg.price * leg.size for leg in self._legs) / total if total > 0 else 0.0

    def _finalize(self, idx: int, price: float, reason: str):
        if not self._legs:
            return
        side = 1 if self._size > 0 else -1
        trade_pnl = 0.0

        for leg in self._legs:
            ex = price - side * self._cfg.slippage
            trade_pnl += (ex - leg.price) * side * leg.size - self._cfg.commission * leg.size * 2

        self._pnl += trade_pnl
        self._trades.append(TradeRecord(
            legs=list(self._legs),
            exit_idx=idx,
            exit_price=price - side * self._cfg.slippage,
            exit_reason=reason,
            total_pnl=trade_pnl,
            vwap_entry=self._vwap,
            side=side,
        ))
        self._legs.clear()
        self._size = 0
        self._vwap = 0.0
