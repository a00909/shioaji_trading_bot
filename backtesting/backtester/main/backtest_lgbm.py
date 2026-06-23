"""
Example 1: Single signal source + fixed TP/SL.

Concept: when fast MA crosses above slow MA, go long 1 lot.
Exit on TP=2.0 or SL=1.5.
"""
from datetime import date

import numpy as np
import pandas as pd
from pyparsing import with_class
from setuptools.command.build_ext import use_stubs

from backtesting.backtester import (
    Backtester, BacktestConfig,
    ModelSignalSource, FixedTPSL,
)
from backtesting.feature_builder.feature_name import FeatureName
from backtesting.lgbm.utils import get_model, thresh_chooser
from backtesting.training_context import TrainingContext
from tools.plotter import Plotter
from backtesting import training_config as cfg

tc = TrainingContext(
    date(2026, 4, 1),
    date(2026, 4, 30),
    cfg.direction_feature_names,
    gen_bars=True,
    bar_interval_seconds=1,
    use_feature_mtx=True
)
sd = tc.fb.sd()
thresh = thresh_chooser(sd)
mask = sd >= thresh

features = tc.features[mask]

model = get_model()
raw = model.predict(features)

tp_rate = raw[:, 2]

# show 分布
df_box = pd.Series(tp_rate)
pd.set_option('display.float_format', lambda x: f'{x:.70f}')
print(df_box.describe(percentiles=[0.25, 0.5, 0.75, 0.99, 0.995, 0.9995]))
# print(np.sum(p_sig == 1))

perc_list = [25, 50, 75, 99.0, 99.5, 99.95]
threshs = [np.percentile(tp_rate, perc) for perc in perc_list]
samples = [np.sum(tp_rate >= thresh) for thresh in threshs]
smp_cnt = [(p, float(t), int(s)) for p, t, s in zip(perc_list, threshs, samples)]
print('idx\tperc\tthresh\tcount')
for idx, (p, t, s) in enumerate(smp_cnt):
    print(f'{idx}\t{p}\t{t}\t{s}')

choose = int(input('choose thresh: '))

signals = (tp_rate >= threshs[choose]).astype(np.int8)

# --- 3. Run backtest -------------------------------------------------
bt = Backtester(
    prices=tc.bars.close[mask],
    features=None,
    components=[
        ModelSignalSource(scores=signals, threshold=1, direction=1),
        FixedTPSL(tp=50, sl=40, trailing=20),
    ],
    config=BacktestConfig(slippage=0.5),
)
result = bt.run()

# --- 4. Inspect output ----------------------------------------------
print('=== Example 1: MA crossover ===')
print('total trades :', result.total_trades)
print('total pnl    :', round(result.total_pnl, 2))
print('win rate     :', f'{result.win_rate:.1%}')
print('max drawdown :', round(result.max_drawdown, 2))
print()
for tr in result.trades[:5]:
    legs = [(l.idx, int(l.size)) for l in tr.legs]
    print(f'  [{tr.side:+d}] entry@{tr.legs[0].price:.2f} -> '
          f'exit@{tr.exit_price:.2f}  legs={legs}  '
          f'pnl={tr.total_pnl:+.2f}  reason={tr.exit_reason}')

for k, v in result.summary().items():
    print(f'{k}\t{v}')

plotter = Plotter(True)

accu = 0
for tr in result.trades:
    accu += tr.total_pnl
    plotter.add_points(
        'reward',
        (tr.exit_idx, accu),

    )
plotter.plot()
