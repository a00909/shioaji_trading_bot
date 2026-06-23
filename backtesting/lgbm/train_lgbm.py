from datetime import date

import numpy as np
import pandas as pd

from backtesting.feature_builder.feature_name import FeatureName
from backtesting.lgbm.dataset_builder import DatasetBuilder
from backtesting.lgbm.lightgbm_trainer import LightGBMTrainer
from backtesting.lgbm.utils import thresh_chooser
from backtesting.training_context import TrainingContext
from backtesting import training_config as cfg

tc = TrainingContext(
    cfg.start,
    cfg.end,
    cfg.direction_feature_names+ [FeatureName.SD],
    **cfg.feature_builder_settings
)

print(f'nan count: {np.isnan(tc.label).sum()}')
print(pd.Series(tc.label).value_counts())
# np.nan_to_num(label, copy=False, nan=0.0)

thresh = thresh_chooser(tc.features[FeatureName.SD])
sd_mask = tc.features[FeatureName.SD] >= thresh
del tc.features[FeatureName.SD]

train_data, valid_data = DatasetBuilder.build2(
    tc.features,
    tc.label + 1,
    [tc.valid,sd_mask],
    tc.bars.ts_seconds(),
    valid_offset_ratio=1,
    valid_ratio=0.2,
    valid_from_start=False,
    sample_interval_seconds=0.0
)

# ── 3. 訓練 ──
trainer = LightGBMTrainer()
trainer.train(train_data, valid_data)

# ── 4. 分析特徵 ──
trainer.analyze_features()

# ── 5. 儲存 ──
# if 'y' == input('save to py?[y].'):
#     trainer.save_py()
#     print('py code saved.')

if 'y' == input('save to txt?[y].'):
    trainer.save()
    print('model saved.')
