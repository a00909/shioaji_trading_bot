"""
Donchian 回測腳本
"""
import sys

from qclaw.backtesting.dc_backtesting_context import DonchianBacktestingContext

sys.path.insert(0, r'/')

import numpy as np
import pandas as pd

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import matplotlib.dates as mdates

import sys

sys.path.insert(0, r'C:\Repos\shioaji_trading')


class BacktestingDC(DonchianBacktestingContext):
    HA_THRESHOLD = 5
    LA_THRESHOLD = 5
    TP_POINTS = 30
    SLIPPAGE = 4
    TRAIL_FRAC_LONG = 0.05
    TRAIL_FRAC_SHORT = 0.05
    OUTPUT_DIR = r'.\results'

    def __init__(self, test_dates):
        super().__init__(test_dates)
        self.records = []

    def _long_signal(self, i):
        return self.has[i] >= self.HA_THRESHOLD and (i == 0 or self.has[i - 1] < self.HA_THRESHOLD)

    def _short_signal(self, i):
        return self.las[i] >= self.LA_THRESHOLD and (i == 0 or self.las[i - 1] < self.LA_THRESHOLD)

    def _entry_check(self, i):
        if self._long_signal(i):
            direction = 1
            return True, direction
        elif self._short_signal(i):
            direction = -1
            return True, direction
        else:
            return False, None

    def _exit_check(self, i, direction):
        # common
        if i >= self.n_total:
            return True
        if i < self.n_total - 1 and self.times[i + 1] - self.times[i] > 60 * 60:
            return True

        # long exit
        if direction == 1:
            if self.prices[i] < (self.hs[i] - self.ls[i]) * self.TRAIL_FRAC_LONG + self.ls[i]:
                return True
        else:
            if self.prices[i] > self.hs[i] - (self.hs[i] - self.ls[i]) * self.TRAIL_FRAC_SHORT:
                return True

        return False

    @staticmethod
    def _plot_style():
        """Dark-theme rcParams + color palette for matplotlib charts."""
        rc = {
            'figure.facecolor': '#0d1117', 'axes.facecolor': '#161b22',
            'text.color': '#8b949e', 'axes.labelcolor': '#8b949e',
            'xtick.color': '#8b949e', 'ytick.color': '#8b949e',
            'font.family': 'monospace',
        }
        C = {
            'bg': '#0d1117', 'axbg': '#161b22', 'grid': '#21262d',
            'text': '#8b949e', 'blue': '#58a6ff', 'red': '#f85149',
            'grn': '#2ecc71', 'ylw': '#f0e68c', 'purple': '#bc8cff',
        }
        return rc, C

    def backtest(self):
        self.records = []

        # 做多
        i = 0
        hold = 0
        direction, entry_price, entry_time, entry_day = None, None, None, None
        peak_price = None
        while i < self.n_total:
            if not hold:
                is_entry, direction = self._entry_check(i)
                if is_entry:
                    entry_price, entry_time, entry_day = self._get_tick_data(i)
                    hold = True
                    peak_price = entry_price
            else:
                is_exit = self._exit_check(i, direction)  # todo: 改成可分批出場的形式

                if direction == 1:
                    peak_price = max(peak_price, self.prices[i])
                else:
                    peak_price = min(peak_price, self.prices[i])

                if is_exit:
                    hold = False

                    exit_price, exit_time, exit_day = self._get_tick_data(i)
                    pnl = direction * (exit_price - entry_price) - self.SLIPPAGE
                    tp_hit = pnl > 0

                    self.records.append({
                        'direction': 'long' if direction == 1 else 'short', 'entry_time': entry_time,
                        'entry_price': entry_price, 'exit_time': exit_time,
                        'exit_price': exit_price, 'peak_price': peak_price,
                        'tp_hit': tp_hit, 'pnl': pnl,
                        'day': entry_time.strftime('%Y-%m-%d'),
                    })

            i += 1

    def results(self, plot=True, write_csv=True, grouped_stat=True, plot_daily_chart=False):
        df = pd.DataFrame(self.records)
        df['entry_time'] = pd.to_datetime(df['entry_time'])
        df = df.sort_values('entry_time').reset_index(drop=True)
        df['cum_pnl'] = df['pnl'].cumsum()
        df['peak'] = df['cum_pnl'].cummax()
        df['drawdown'] = df['peak'] - df['cum_pnl']

        n_long = int((df.direction == 'long').sum())
        n_short = int((df.direction == 'short').sum())
        n_trades = len(df)
        ev_long = float(df.loc[df.direction == 'long', 'pnl'].mean())
        ev_short = float(df.loc[df.direction == 'short', 'pnl'].mean())
        total_pnl = float(df['cum_pnl'].iloc[-1])
        avg_pnl = float(df['pnl'].mean())
        win_rate = float((df['pnl'] > 0).mean()) * 100
        tp_rate = float(df['tp_hit'].mean()) * 100
        max_win = float(df['pnl'].max())
        max_loss = float(df['pnl'].min())
        max_dd_trade = float(df['drawdown'].max())

        # 每日 PnL
        daily = df.groupby('day')['pnl'].sum()
        daily.index = pd.to_datetime(daily.index)
        daily = daily.sort_index()
        daily_cum = daily.cumsum()
        daily_dd = daily_cum.cummax() - daily_cum
        n_days = daily.index.nunique()
        avg_daily = float(daily.mean())
        min_daily = float(daily.min())
        max_daily = float(daily.max())
        n_losing = int((daily < 0).sum())
        max_dd_day = float(daily_dd.max())

        print(f'trades: {n_trades} (long={n_long}, short={n_short})')
        print(f'ha EV: {ev_long:.2f} pts/trade')
        print(f'la EV: {ev_short:.2f} pts/trade')
        print(f'total PnL: {total_pnl:,.0f} pts')
        print(f'avg PnL: {avg_pnl:.2f} pts/trade')
        print(f'win rate: {win_rate:.1f}%')
        print(f'TP rate: {tp_rate:.1f}%')
        print(f'max win: +{max_win:.0f}, max loss: {max_loss:.0f}')
        print(f'max trade dd: {max_dd_trade:.0f} pts')
        print(f'max daily dd: {max_dd_day:.0f} pts')
        print(f'losing days: {n_losing}/{n_days}')

        if plot:
            plt.rcParams.update({
                'figure.facecolor': '#0d1117', 'axes.facecolor': '#161b22',
                'text.color': '#8b949e', 'axes.labelcolor': '#8b949e',
                'xtick.color': '#8b949e', 'ytick.color': '#8b949e',
                'font.family': 'monospace',
            })
            BG = '#0d1117'
            AXBG = '#161b22'
            GRID = '#21262d'
            TEXT = '#8b949e'
            BLUE = '#58a6ff'
            RED = '#f85149'
            GRN = '#2ecc71'
            YLW = '#f0e68c'

            fig = plt.figure(figsize=(18, 16))
            gs = GridSpec(4, 1, figure=fig, hspace=0.45, height_ratios=[3, 1.2, 1.2, 1.2])
            ax1 = fig.add_subplot(gs[0])
            ax2 = fig.add_subplot(gs[1])
            ax3 = fig.add_subplot(gs[2])
            ax4 = fig.add_subplot(gs[3])
            for ax in [ax1, ax2, ax3, ax4]:
                ax.set_facecolor(AXBG)
                for s in ['bottom', 'top', 'left', 'right']:
                    ax.spines[s].set_color(GRID)

            # ax1: equity curve
            x = df['entry_time']
            y = df['cum_pnl']
            ax1.fill_between(x, y, alpha=0.12, color=BLUE)
            ax1.plot(x, y, color=BLUE, linewidth=1.0, zorder=3)
            ax1.axhline(0, color=TEXT, linewidth=0.5, linestyle='--', alpha=0.5)
            dd_idx = df['drawdown'].idxmax()
            ax1.axvline(df.loc[dd_idx, 'entry_time'], color=RED, linewidth=0.8, linestyle='--', alpha=0.7)
            ax1.scatter([df.loc[dd_idx, 'entry_time']], [df.loc[dd_idx, 'cum_pnl']], color=RED, s=30, zorder=5)
            ax1.annotate(f'Max DD: {max_dd_trade:.0f} pts',
                         xy=(df.loc[dd_idx, 'entry_time'], df.loc[dd_idx, 'cum_pnl']),
                         xytext=(8, -20), textcoords='offset points',
                         color=RED, fontsize=8,
                         arrowprops=dict(arrowstyle='->', color=RED, lw=0.8))
            ax1.set_title(
                f'Donchian 60-Day  |  {n_trades} trades  |  '
                f'TP {tp_rate:.1f}%  Win {win_rate:.1f}%  |  '
                f'EV {avg_pnl:.1f} pts/trade  |  '
                f'Total +{total_pnl:,.0f} pts  |  '
                f'MaxDD {max_dd_trade:.0f}  |  LosingDays {n_losing}/{n_days}',
                color='#e6edf3', fontsize=10, pad=10)
            ax1.set_ylabel('Cumulative PnL (pts)', color=TEXT, fontsize=9)
            ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax1.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax1.grid(True, color=GRID, linewidth=0.5, zorder=0)

            # ax2: drawdown
            ax2.fill_between(x, -df['drawdown'], 0, color=RED, alpha=0.5)
            ax2.plot(x, -df['drawdown'], color=RED, linewidth=0.7)
            ax2.set_ylabel('Drawdown (pts)', color=TEXT, fontsize=9)
            ax2.set_title(
                f'Drawdown  |  Max: {max_dd_trade:.0f} pts (trade)  |  {max_dd_day:.0f} pts (daily)',
                color='#e6edf3', fontsize=9, pad=6)
            ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f'{v:,.0f}'))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
            plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax2.grid(True, color=GRID, linewidth=0.5)

            # ax3: daily PnL
            bcols = [RED if p < 0 else GRN for p in daily.values]
            ax3.bar(daily.index, daily.values, color=bcols, width=0.8, alpha=0.85, zorder=3)
            ax3.axhline(avg_daily, color=YLW, linewidth=0.9, linestyle='--', label=f'Avg {avg_daily:.0f}')
            ax3.axhline(0, color=TEXT, linewidth=0.5)
            ax3.set_ylabel('Daily PnL (pts)', color=TEXT, fontsize=9)
            ax3.set_title(
                f'Daily PnL  |  Avg {avg_daily:.0f}  |  '
                f'Min {min_daily:.0f}  Max {max_daily:.0f}  |  LosingDays {n_losing}/{n_days}',
                color='#e6edf3', fontsize=9, pad=6)
            ax3.legend(facecolor=AXBG, edgecolor=GRID, labelcolor=YLW, fontsize=8)
            ax3.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            ax3.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=0))
            plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax3.grid(True, color=GRID, linewidth=0.5, axis='y', zorder=0)

            # ax4: PnL histogram (both directions)
            lo_mask = df['direction'] == 'long'
            sh_mask = df['direction'] == 'short'
            bins = np.arange(df['pnl'].min() - 5, df['pnl'].max() + 10, 5)
            ax4.hist(df.loc[lo_mask, 'pnl'], bins=bins, color=GRN, alpha=0.6, label=f'Long ({lo_mask.sum()})')
            ax4.hist(df.loc[sh_mask, 'pnl'], bins=bins, color=BLUE, alpha=0.6, label=f'Short ({sh_mask.sum()})')
            ax4.axvline(avg_pnl, color=YLW, linewidth=1.2, linestyle='--', label=f'Avg {avg_pnl:.1f}')
            ax4.set_xlabel('PnL per trade (pts)', color=TEXT, fontsize=9)
            ax4.set_ylabel('Count', color=TEXT, fontsize=9)
            ax4.set_title(
                f'PnL Distribution  |  {n_trades} trades  |  '
                f'Max win +{max_win:.0f}  |  Max loss {max_loss:.0f}',
                color='#e6edf3', fontsize=9, pad=6)
            ax4.legend(facecolor=AXBG, edgecolor=GRID, labelcolor=TEXT, fontsize=8)
            ax4.grid(True, color=GRID, linewidth=0.5, axis='y', zorder=0)

            plt.savefig(f'{self.OUTPUT_DIR}\\equity.png', dpi=150, bbox_inches='tight', facecolor=BG)
            plt.close()
            print(f'\nSaved: {self.OUTPUT_DIR}\\equity.png')

        if write_csv:
            # 寫 CSV
            df.to_csv(f'{self.OUTPUT_DIR}\\trade_records.csv', index=False)
            daily.to_csv(f'{self.OUTPUT_DIR}\\daily_pnl.csv', header=['pnl'])
            print(f'Saved: trade_records.csv, daily_pnl.csv')

        if grouped_stat:
            print('\n--- by direction ---')
            for name, g in df.groupby('direction'):
                arr = g['pnl'].values
                print(f'  {name:5s}: n={len(arr):3d}, EV={arr.mean():+7.2f}, '
                      f'win={sum(arr > 0) / len(arr) * 100:5.1f}%, '
                      f'max_win={arr.max():+7.1f}, max_loss={arr.min():+7.1f}')

            print('\n--- by volatility ---')
            for lo, hi, name in [(0, 480, 'low'), (480, 680, 'mid'), (680, 9999, 'high')]:
                pnls = [row['pnl'] for _, row in df.iterrows()
                        if lo <= self.day_ranges.get(row['day'], 0) < hi]
                if pnls:
                    arr = np.array(pnls)
                    print(f'  {name:4s}: n={len(arr):3d}, EV={arr.mean():+7.2f}, '
                          f'win={sum(arr > 0) / len(arr) * 100:5.1f}%, '
                          f'max_win={arr.max():+7.1f}, max_loss={arr.min():+7.1f}')

        if plot_daily_chart:
            rc, C = self._plot_style()
            plt.rcParams.update(rc)
            import os
            os.makedirs(self.OUTPUT_DIR, exist_ok=True)

            unique_days = sorted(set(self.days))
            for day in unique_days:
                mask = self.days == day
                day_idx = np.where(mask)[0]
                t = self.idx[day_idx]
                price = self.prices[day_idx]
                h = self.hs[day_idx]
                l = self.ls[day_idx]
                ha = self.has[day_idx]
                la = self.las[day_idx]

                # 用時間範圍匹配交易（正確處理夜盤跨日）
                t_min, t_max = t.iloc[0], t.iloc[-1]
                day_trades = [r for r in self.records
                              if t_min <= r['entry_time'] <= t_max]

                # 雙層：上圖走勢，下圖 ha/la
                fig = plt.figure(figsize=(18, 10))
                gs = GridSpec(2, 1, figure=fig, hspace=0.40, height_ratios=[3, 1])
                ax1 = fig.add_subplot(gs[0])
                ax2 = fig.add_subplot(gs[1])

                for ax in [ax1, ax2]:
                    ax.set_facecolor(C['axbg'])
                    for s in ['bottom', 'top', 'left', 'right']:
                        ax.spines[s].set_color(C['grid'])

                # ── 上圖：走勢 ──
                ax1.fill_between(t, l, h, alpha=0.10, color=C['purple'], zorder=1)
                ax1.plot(t, h, color=C['purple'], linewidth=0.5, alpha=0.4, zorder=2)
                ax1.plot(t, l, color=C['purple'], linewidth=0.5, alpha=0.4, zorder=2)
                ax1.plot(t, price, color=C['blue'], linewidth=0.8, zorder=3)

                for r in day_trades:
                    is_long = r['direction'] == 'long'
                    ec = C['grn'] if is_long else C['red']
                    mk = '^' if is_long else 'v'
                    ax1.scatter(r['entry_time'], r['entry_price'],
                                marker=mk, color=ec, s=80, zorder=5,
                                edgecolors='white', linewidths=0.5)
                    ax1.scatter(r['exit_time'], r['exit_price'],
                                marker='o', color=ec, s=40, zorder=5,
                                edgecolors='white', linewidths=0.5)
                    ax1.plot([r['entry_time'], r['exit_time']],
                             [r['entry_price'], r['exit_price']],
                             color=ec, linewidth=0.6, linestyle='--', alpha=0.5, zorder=4)
                    mid_t = r['entry_time'] + (r['exit_time'] - r['entry_time']) / 2
                    mid_p = (r['entry_price'] + r['exit_price']) / 2
                    ax1.annotate(f"{r['pnl']:+.0f}", xy=(mid_t, mid_p),
                                 fontsize=7, color=ec, ha='center', va='bottom')

                day_range = float(price.max() - price.min())
                n_day_trades = len(day_trades)
                day_pnl = sum(r['pnl'] for r in day_trades)
                ax1.set_title(
                    f'{day}  |  Range {day_range:.0f} pts  |  '
                    f'{n_day_trades} trades  |  PnL {day_pnl:+.0f} pts',
                    color='#e6edf3', fontsize=10, pad=10)
                ax1.set_ylabel('Price (pts)', color=C['text'], fontsize=9)
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))
                plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
                ax1.grid(True, color=C['grid'], linewidth=0.5, zorder=0)

                # ── 下圖：ha / la ──
                ha_vals = ha.values if hasattr(ha, 'values') else ha
                la_vals = la.values if hasattr(la, 'values') else la
                bar_w = (t.iloc[1] - t.iloc[0]).total_seconds() / 3600 * 0.8 if len(t) > 1 else 0.05
                bar_w = max(bar_w, 0.05)

                # ha：正值（綠），la：負值（紅），畫在同一張 bar
                bars_ha = ax2.bar(t, ha_vals, width=bar_w, color=C['grn'], alpha=0.75, zorder=3,
                                  label=f'HA ({self.HA_THRESHOLD})')
                bars_la = ax2.bar(t, [-v for v in la_vals], width=bar_w, color=C['red'], alpha=0.75, zorder=3,
                                  label=f'LA ({self.LA_THRESHOLD})')
                ax2.axhline(0, color=C['grid'], linewidth=0.5, zorder=2)
                ax2.axhline(self.HA_THRESHOLD, color=C['grn'], linewidth=0.8, linestyle='--', alpha=0.6)
                ax2.axhline(-self.LA_THRESHOLD, color=C['red'], linewidth=0.8, linestyle='--', alpha=0.6)
                ax2.set_ylabel('HA/LA (pts)', color=C['text'], fontsize=9)
                ax2.set_xlabel('Time', color=C['text'], fontsize=9)
                ax2.legend(facecolor=C['axbg'], edgecolor=C['grid'], labelcolor=C['text'], fontsize=8,
                           loc='upper right')
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                ax2.xaxis.set_major_locator(mdates.HourLocator(interval=1))
                plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
                ax2.grid(True, color=C['grid'], linewidth=0.5, axis='y', zorder=0)

                path = f'{self.OUTPUT_DIR}\\daily_{day}.png'
                fig.savefig(path, dpi=150, bbox_inches='tight', facecolor=C['bg'])
                plt.close(fig)
                print(f'  Saved: {path}')

        print('\nDone.')
