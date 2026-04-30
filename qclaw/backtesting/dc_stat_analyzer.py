"""
DonchianStat 紀錄分布分析
按 (direction, threshold) 分組，統計 max_accum × max_possible_pnl 聯合分布
以及 MAE × EV 掃描
"""
import io
import sys
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from qclaw.backtesting.data.dc_stat_record import DonchianStatRecord


# ─────────────────────────────────────────────────────────
# 統計函數（純計算，無副作用）
# ─────────────────────────────────────────────────────────

def compute_crosstab(
    df: pd.DataFrame,
    accu_bins: list,
    accu_labels: list,
    pnl_bins: list,
    pnl_labels: list
) -> pd.DataFrame:
    """
    計算聯合分布矩陣（百分比）。

    Parameters
    ----------
    df : pd.DataFrame
        已篩選過的子集（含 max_accum, abs_pnl 欄位）
    accu_bins, accu_labels : list
        max_accum 的 bin edges 和 labels
    pnl_bins, pnl_labels : list
        abs_pnl 的 bin edges 和 labels

    Returns
    -------
    pd.DataFrame
        聯合分布矩陣（百分比），index=accu_bin, columns=pnl_bin
    """
    df = df.copy()
    df['accu_bin'] = pd.cut(df['max_accum'], bins=accu_bins, labels=accu_labels, right=False)
    df['pnl_bin'] = pd.cut(df['abs_pnl'], bins=pnl_bins, labels=pnl_labels, right=False)
    ct = pd.crosstab(df['accu_bin'], df['pnl_bin'], normalize='all') * 100
    return ct


def compute_summary_stats(df: pd.DataFrame) -> dict:
    """
    計算摘要統計。

    Parameters
    ----------
    df : pd.DataFrame
        已篩選過的子集（含 abs_pnl, max_accum, mae 欄位）

    Returns
    -------
    dict
        {
            'abs_pnl': {'avg': ..., 'median': ..., 'std': ...},
            'max_accum': {...},
            'mae': {..., 'max': ...},
            'n': int
        }
    """
    return {
        'n': len(df),
        'abs_pnl': {
            'avg': df['abs_pnl'].mean(),
            'median': df['abs_pnl'].median(),
            'std': df['abs_pnl'].std(),
        },
        'max_accum': {
            'avg': df['max_accum'].mean(),
            'median': df['max_accum'].median(),
            'std': df['max_accum'].std(),
        },
        'mae': {
            'avg': df['mae'].mean(),
            'median': df['mae'].median(),
            'std': df['mae'].std(),
            'max': df['mae'].max(),
        },
    }


def compute_ev_table(
    df: pd.DataFrame,
    tp_grid: list[int],
    sl_grid: list[int],
    ignore_penalty_enabled: bool,
    ignore_penalty_points: int
) -> list[tuple]:
    """
    計算 EV 表。

    Parameters
    ----------
    df : pd.DataFrame
        已篩選過的子集（含 max_possible_pnl, mae 欄位）
    tp_grid : list[int]
        止盈點數列表
    sl_grid : list[int]
        止損點數列表
    ignore_penalty_enabled : bool
        是否啟用 ignore 懲罰
    ignore_penalty_points : int
        每筆 ignore 扣的點數（負數）

    Returns
    -------
    list[tuple]
        [(TP, SL, win_rate, losses, wins, ignore, EV), ...]
        已按 EV 降序排列
    """
    n = len(df)
    if n == 0:
        return []

    results = []
    for tp in tp_grid:
        for sl in sl_grid:
            if sl >= tp:
                continue
            wins = ((df['max_possible_pnl'] >= tp) & (df['mae'] < sl)).sum()
            losses = (df['mae'] >= sl).sum()
            ignore = n - wins - losses
            win_rate = wins / n
            loss_rate = losses / n

            if ignore_penalty_enabled:
                ev = win_rate * tp - loss_rate * sl - (ignore / n) * abs(ignore_penalty_points)
            else:
                ev = win_rate * tp - loss_rate * sl

            results.append((tp, sl, win_rate, losses, wins, ignore, ev))

    results.sort(key=lambda x: x[6], reverse=True)
    return results


def compute_flat_list(
    crosstab: pd.DataFrame,
    direction: str,
    threshold: int
) -> list[tuple]:
    """
    從 Crosstab 轉成逐筆列表。

    Parameters
    ----------
    crosstab : pd.DataFrame
        compute_crosstab() 的輸出
    direction : str
    threshold : int

    Returns
    -------
    list[tuple]
        [(direction, threshold, accu_bin, pnl_bin, pct), ...]
    """
    results = []
    for accu_bin in crosstab.index:
        for pnl_bin in crosstab.columns:
            pct = crosstab.loc[accu_bin, pnl_bin]
            if pct > 0:
                results.append((direction, threshold, str(accu_bin), str(pnl_bin), pct))
    return results


# ─────────────────────────────────────────────────────────
# 主類別
# ─────────────────────────────────────────────────────────

class DonchianStatAnalyzer:
    """
    DonchianStat 紀錄分布分析器

    分析 max_accum × max_possible_pnl 聯合分布，並掃描 (TP, SL) 組合的期望值。
    """

    # 類別常數（Grid 參數）
    TP_GRID = [10, 15, 20, 30, 40, 50, 60, 80, 100, 150, 300, 500, 800, 1000]
    SL_GRID = [10, 15, 20, 30, 40, 50, 60, 80, 100, 150]
    IGNORE_PENALTY_ENABLED = True
    IGNORE_PENALTY_POINTS = -45

    def __init__(
        self,
        records: list[DonchianStatRecord],
        thresholds: list[int],
        n_days: Optional[int] = None,
        output_dir: str = 'results'
    ):
        self.records = records
        self.thresholds = thresholds
        self.output_dir = output_dir

        if n_days is None:
            days = set(r.day for r in records)
            self.n_days = len(days)
        else:
            self.n_days = n_days

        # 預處理
        self._df = self._build_dataframe()
        self._accu_bins, self._accu_labels, self._pnl_bins, self._pnl_labels = \
            self._build_bins(thresholds)

    # ─────────────────────────────────────────────────────────
    # 公開方法
    # ─────────────────────────────────────────────────────────
    def compute(self) -> dict:
        """
        純計算，無副作用。

        Returns
        -------
        dict
            {
                'crosstabs': {(direction, threshold): DataFrame, ...},
                'summary_stats': {(direction, threshold): dict, ...},
                'ev_tables': {(direction, threshold): list[tuple], ...},
                'flat_lists': {(direction, threshold): list[tuple], ...},
            }
        """
        stats = {
            'crosstabs': {},
            'summary_stats': {},
            'ev_tables': {},
            'flat_lists': {},
        }

        for direction in ['long', 'short']:
            for threshold in self.thresholds:
                sub = self._df[(self._df['direction'] == direction) &
                               (self._df['threshold'] == threshold)]

                key = (direction, threshold)

                # Crosstab
                ct = compute_crosstab(
                    sub, self._accu_bins, self._accu_labels,
                    self._pnl_bins, self._pnl_labels
                )
                stats['crosstabs'][key] = ct

                # Summary stats
                stats['summary_stats'][key] = compute_summary_stats(sub)

                # EV table
                ev = compute_ev_table(
                    sub, self.TP_GRID, self.SL_GRID,
                    self.IGNORE_PENALTY_ENABLED, self.IGNORE_PENALTY_POINTS
                )
                stats['ev_tables'][key] = ev

                # Flat list（從 crosstab 轉換）
                stats['flat_lists'][key] = compute_flat_list(ct, direction, threshold)

        return stats

    def report(self, stats: dict) -> None:
        """
        純輸出，接收 compute() 的結果。
        """
        self._print_crosstabs(stats)
        self._print_ev_tables(stats)
        self._plot_heatmaps(stats)
        self._print_flat_lists(stats)

    def analyze(self, save_file: bool = True) -> None:
        """
        主入口：計算 + 輸出。
        """
        buffer = io.StringIO()
        old_stdout = sys.stdout
        sys.stdout = buffer

        try:
            stats = self.compute()
            self.report(stats)
        finally:
            sys.stdout = old_stdout

        output = buffer.getvalue()
        print(output, end='')

        if save_file:
            self._save(output)

    # ─────────────────────────────────────────────────────────
    # 內部方法：資料建構
    # ─────────────────────────────────────────────────────────
    def _build_dataframe(self) -> pd.DataFrame:
        rows = [r.to_dict() for r in self.records]
        df = pd.DataFrame(rows)
        df['abs_pnl'] = df['max_possible_pnl']
        return df

    def _build_bins(self, thresholds: list[int]):
        accu_bins = sorted(set([0] + list(thresholds) + [np.inf]))
        accu_labels = []
        for i in range(len(accu_bins) - 1):
            lo = accu_bins[i]
            hi = accu_bins[i + 1]
            if hi == np.inf:
                accu_labels.append(f'{lo}+')
            else:
                accu_labels.append(f'{lo}-{hi}')

        pnl_bins = [0, 30, 60, 100, 200, 500, 800, 1000, np.inf]
        pnl_labels = ['0-30', '30-60', '60-100', '100-200', '200-500', '500-800', '800-1000', '1000+']

        return accu_bins, accu_labels, pnl_bins, pnl_labels

    # ─────────────────────────────────────────────────────────
    # 內部方法：輸出
    # ─────────────────────────────────────────────────────────
    def _print_crosstabs(self, stats: dict) -> None:
        for direction in ['long', 'short']:
            print(f'\n{"=" * 70}')
            print(f'  {direction.upper()}')
            print(f'{"=" * 70}')

            for threshold in self.thresholds:
                key = (direction, threshold)
                ct = stats['crosstabs'][key]
                summary = stats['summary_stats'][key]
                self._print_crosstab_for(ct, summary, direction, threshold)

    def _print_crosstab_for(
        self,
        crosstab: pd.DataFrame,
        summary: dict,
        direction: str,
        threshold: int
    ) -> None:
        n = summary['n']
        if n == 0:
            print(f'\n  threshold={threshold}: no records')
            return

        print(f'\n  threshold={threshold}  (n={n})')

        # 表頭
        col_width = 10
        corner = r'accu\pnl'
        header = f'{corner:>{col_width}}'
        for col in crosstab.columns:
            header += f'{str(col):>{col_width}}'
        header += f'{"row%":>{col_width}}'
        print(header)
        print('-' * len(header))

        # 每行
        for idx, row in crosstab.iterrows():
            line = f'{str(idx):>{col_width}}'
            row_sum = 0
            for col in crosstab.columns:
                val = row[col]
                row_sum += val
                line += f'{val:>9.1f}%'
            line += f'{row_sum:>9.1f}%'
            print(line)

        # 欄合計
        total_line = f'{"col%":>{col_width}}'
        for col in crosstab.columns:
            col_sum = crosstab[col].sum()
            total_line += f'{col_sum:>9.1f}%'
        total_line += f'{crosstab.values.sum():>9.1f}%'
        print('-' * len(header))
        print(total_line)

        # 摘要統計
        s = summary
        print(f'\n  abs_pnl    avg={s["abs_pnl"]["avg"]:.1f}  '
              f'median={s["abs_pnl"]["median"]:.1f}  '
              f'std={s["abs_pnl"]["std"]:.1f}')
        print(f'  max_accum  avg={s["max_accum"]["avg"]:.1f}  '
              f'median={s["max_accum"]["median"]:.1f}  '
              f'std={s["max_accum"]["std"]:.1f}')
        print(f'  mae        avg={s["mae"]["avg"]:.1f}  '
              f'median={s["mae"]["median"]:.1f}  '
              f'std={s["mae"]["std"]:.1f}  '
              f'max={s["mae"]["max"]:.0f}')

    def _print_ev_tables(self, stats: dict) -> None:
        print(f'\n{"=" * 70}')
        print('  EV 掃描  (TP, SL) → 勝率 / EV')
        print(f'{"=" * 70}')
        print(f'  TP grid: {self.TP_GRID}')
        print(f'  SL grid: {self.SL_GRID}')
        if self.IGNORE_PENALTY_ENABLED:
            print(f'  [IGNORE PENALTY] enabled: 每筆 ignore 扣 {self.IGNORE_PENALTY_POINTS} 點')
        else:
            print(f'  [IGNORE PENALTY] disabled: ignore 視為 0 點')

        for direction in ['long', 'short']:
            print(f'\n  --- {direction.upper()} ---')
            for threshold in self.thresholds:
                key = (direction, threshold)
                ev_table = stats['ev_tables'][key]
                summary = stats['summary_stats'][key]
                self._print_ev_table_for(ev_table, summary['n'], direction, threshold)

    def _print_ev_table_for(
        self,
        ev_table: list[tuple],
        n: int,
        direction: str,
        threshold: int
    ) -> None:
        if n == 0:
            return

        top = ev_table[:10]
        print(f'\n  threshold={threshold}  (n={n})  TOP-10 EV：')
        hdr = f'  {"TP":>4}  {"SL":>4}  {"WIN%":>7}  {"lose":>6}  {"win":>6}  {"ign%":>6}  {"ign#":>5}  {"EV":>8}'
        print(hdr)
        print('  ' + '-' * (len(hdr) - 2))
        for tp, sl, wr, losses, wins, ignore, ev in top:
            marker = '  ← POSITIVE EV' if ev > 0 else ''
            ignore_pct = ignore / n * 100
            print(f'  {tp:>4}  {sl:>4}  {wr*100:>6.1f}%  {losses:>6}  {wins:>6}  '
                  f'{ignore_pct:>5.1f}%  {ignore:>5}  {ev:>8.1f}{marker}')

    def _print_flat_lists(self, stats: dict) -> None:
        print(f'\n{"=" * 70}')
        print('  FLAT LIST  (閾值, max_accu, max_pnl) → %')
        print(f'{"=" * 70}')

        for direction in ['long', 'short']:
            for threshold in self.thresholds:
                key = (direction, threshold)
                flat_list = stats['flat_lists'][key]
                for d, t, accu_bin, pnl_bin, pct in flat_list:
                    print(f'  ({d} threshold={t}, max_accu={accu_bin}, max_pnl={pnl_bin}): {pct:.1f}%')

    def _plot_heatmaps(self, stats: dict) -> None:
        import os
        os.makedirs(self.output_dir, exist_ok=True)

        for direction in ['long', 'short']:
            self._plot_heatmap_for(stats, direction)

    def _plot_heatmap_for(self, stats: dict, direction: str) -> None:
        import os

        # 找出有資料的 threshold
        valid_thresholds = []
        for threshold in self.thresholds:
            key = (direction, threshold)
            if stats['summary_stats'][key]['n'] > 0:
                valid_thresholds.append(threshold)

        if not valid_thresholds:
            return

        n_thresholds = len(valid_thresholds)
        fig, axes = plt.subplots(1, n_thresholds, figsize=(5 * n_thresholds, 5), squeeze=False)

        for ax_idx, threshold in enumerate(valid_thresholds):
            key = (direction, threshold)
            ct = stats['crosstabs'][key]
            n = stats['summary_stats'][key]['n']

            # Reindex 確保所有 bin 都出現
            ct = ct.reindex(index=self._accu_labels, columns=self._pnl_labels, fill_value=0)

            ax = axes[0][ax_idx]
            sns.heatmap(ct, annot=True, fmt='.1f', cmap='YlOrRd', ax=ax,
                        linewidths=0.5, cbar=ax_idx == n_thresholds - 1,
                        vmin=0, vmax=100)
            ax.set_title(f'threshold={threshold}  n={n}', fontsize=10)
            ax.set_xlabel('max_pnl (pts)')
            ax.set_ylabel('max_accum')
            ax.tick_params(axis='x', rotation=30)
            ax.tick_params(axis='y', rotation=0)

        fig.suptitle(f'DC Stat Distribution — {direction.upper()}', fontsize=13, y=1.02)
        fig.tight_layout()
        path = os.path.join(self.output_dir, f'dc_stat_{direction}.png')
        fig.savefig(path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        print(f'  Saved: {path}')

    def _save(self, output: str) -> None:
        import os
        ts = datetime.now().strftime('%Y%m%d%H%M%S')
        fname = f'result_dc_stat_{self.n_days}d_{ts}.txt'
        path = os.path.join(self.output_dir, fname)
        os.makedirs(self.output_dir, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f'\n  [已存檔] {path}')


# ─────────────────────────────────────────────────────────
# 向後相容
# ─────────────────────────────────────────────────────────
def analyze_dc_stat(records, thresholds, n_days=None, save_file=True):
    """向後相容的函數介面。"""
    analyzer = DonchianStatAnalyzer(records, thresholds, n_days=n_days)
    analyzer.analyze(save_file=save_file)
