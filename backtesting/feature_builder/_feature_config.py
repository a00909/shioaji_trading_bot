from dataclasses import dataclass
from datetime import timedelta


@dataclass
class FeatureConfig:
    """特徵計算設定"""
    # 時間窗口設定
    net_buy_window_s: timedelta = timedelta(minutes=15)  # 短
    net_buy_window_m: timedelta = timedelta(minutes=30)  # 中
    net_buy_window_l: timedelta = timedelta(minutes=60)  # 長
    sd_window: timedelta = timedelta(minutes=45)  # 標準差窗口
    momentum_window_short: timedelta = timedelta(minutes=5)  # 短動量
    momentum_window_long: timedelta = timedelta(minutes=15)  # 長動量

    # IIVA 設定
    iiva_length: timedelta = timedelta(days=5)
    iiva_interval_minutes: int = 5

    # volume_ratio 窗口
    volume_ratio_window: timedelta = timedelta(minutes=5)

    # Donchian 累計設定
    donchian_window: timedelta = timedelta(minutes=30)

    pnl_label_window:timedelta = timedelta(minutes=30)