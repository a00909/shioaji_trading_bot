from datetime import date

from backtesting.feature_builder.feature_name import FeatureName
from backtesting.feature_grapher import FeatureGrapher

fg = FeatureGrapher()
fg.graph(
    date(2025, 11, 3),
    date(2025, 11, 3),
    {
        FeatureName.PRICE: 0,
        FeatureName.DONCHIAN_L: 0,
        FeatureName.DONCHIAN_H: 0,
        'ba_imb':1,
        # FeatureName.DC_BRKOUT_ACCU: 1,
        # FeatureName.DONCHIAN_LA: 1,
        # FeatureName.DONCHIAN_HA: 1,
        # FeatureName.COS_TIME: 2,
        # FeatureName.SIN_TIME: 2,
        # FeatureName.DIR_SD:3
    }
)
