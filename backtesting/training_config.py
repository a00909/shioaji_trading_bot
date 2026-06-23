from datetime import date

from backtesting.feature_builder.feature_name import FeatureName

start = date(2025, 11, 1)
end = date(2026, 3, 28)
power_feature_names = [
    FeatureName.DIR_SD,
]
direction_feature_names = [
    # FeatureName.SD,
    FeatureName.SIN_TIME,
    FeatureName.COS_TIME,
    FeatureName.VOLUME_RATIO,
    # FeatureName.NET_BUY_RATIO_L,
    # FeatureName.NET_BUY_RATIO_M,
    FeatureName.NET_BUY_RATIO_S,
    # FeatureName.BID_ASK_DIFF,
    # FeatureName.BID_ASK_IMBALANCE,
    # FeatureName.HOUR_MINUTE,
    FeatureName.DONCHIAN_HA,
    FeatureName.DONCHIAN_LA,
    # FeatureName.DC_BRKOUT_ACCU
    # 'dc_energy'
    'ba_imb',
    'ba_imb_cr',
]
feature_builder_settings = dict(
    with_label=True,
    gen_bars=True,
    bar_interval_seconds=1
)
