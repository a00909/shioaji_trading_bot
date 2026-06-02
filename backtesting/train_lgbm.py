from datetime import date

from backtesting.dataset_builder import DatasetBuilder
from backtesting.feature_builder._feature_config import FeatureConfig
from backtesting.feature_builder.feature_builder import FeatureBuilder
from backtesting.feature_builder.feature_name import FeatureName
from backtesting.feature_builder.labeler import combine_label
from backtesting.lightgbm_trainer import LightGBMTrainer
from data_manager.history.statics.np_ticks import NPTicks
from tools.backtesting_context import BacktestingContext
from tools.utils import tmf_r1_contract

BTC = BacktestingContext()
app = BTC.app
htm = BTC.npy_htm
with app.raw_connection as conn:
    daily_slices = htm.get(conn, tmf_r1_contract(app.api), date(2025, 11, 1), date(2026, 3, 28))
slices: list[NPTicks] = [s.tick_slice for s in daily_slices if s.tick_slice is not None]

ticks = NPTicks.merge_slices(slices)

print('ticks load.')

fb = FeatureBuilder(ticks, BTC.iiva_lookup, FeatureConfig())
f = fb.build([
    # FeatureName.NET_BUY_RATIO_L,
    # FeatureName.NET_BUY_RATIO_M,
    # FeatureName.NET_BUY_RATIO_S,
    # FeatureName.VOLUME_RATIO,
    # FeatureName.SD,
    # FeatureName.BID_ASK_DIFF,
    # FeatureName.BID_ASK_IMBALANCE,
    # FeatureName.HOUR_MINUTE,
    # FeatureName.DONCHIAN_HA,
    # FeatureName.DONCHIAN_LA,
    # FeatureName.SIN_TIME,
    # FeatureName.COS_TIME,
    # FeatureName.IS_OP_30,
    # FeatureName.IS_CL_30,
    # FeatureName.DIR_SD,
    # FeatureName.DC_BRKOUT_ACCU
    # 'dc_energy'
    # 'ba_imb',
    'ba_imb_cr',

], with_label=True)

print('feature built.')

features = {}
label_long = None
label_short = None
valid_mask = None
for k, v in f.items():
    if k == FeatureName.MAX_FAV:
        label_long = v
    elif k == FeatureName.MAX_ADV:
        label_short = v
    elif k == FeatureName.VALID_MASK:
        valid_mask = v
    else:
        features[k] = v

label = combine_label(label_long, label_short)

train_data, valid_data = DatasetBuilder.build(
    features,
    label,
    valid_mask,
    valid_offset_ratio=1,
    valid_ratio=0.2,
    valid_from_start=False
)

# ── 3. 訓練 ──
trainer = LightGBMTrainer()
trainer.train(train_data, valid_data)

# ── 4. 分析特徵 ──
trainer.analyze_features()

# ── 5. 儲存 ──
# trainer.save('model.txt')
# print('Saved: model.txt')
