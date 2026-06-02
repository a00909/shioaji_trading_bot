import numpy as np
import pandas as pd

from backtesting.feature_builder.feature_name import FeatureName


def extract_time_features_util(times: np.ndarray) -> dict[str, np.ndarray]:
    """
    時間特徵工程工具函式 (24小時全天候標準唯一映射版，完美平滑過渡)

    Args:
        times (np.ndarray): 一維 float64 陣列，內容為 Unix 時間戳（單位：秒）

    Returns:
        dict[str, np.ndarray]: 包含各個時間特徵一維 ndarray 的字典
    """
    # 1. 向量化將時間戳轉換為台灣時區的 DatetimeIndex
    dt_index = pd.to_datetime(times, unit='s', utc=True).tz_convert('Asia/Taipei')

    # 2. 獲取絕對的小時與分鐘
    hour = dt_index.hour.to_numpy()
    minute = dt_index.minute.to_numpy()

    # 3. 計算當天從 00:00 開始的絕對分鐘數 (範圍嚴格落在 0 ~ 1399 之間)
    # 這一步徹底解決了跨日、過0點的不連續斷點問題，因為時間軸永遠是平滑循環的
    absolute_minutes_of_day = hour * 60 + minute

    # --- 核心修正：將映射基底改為一整天（1440分鐘） ---
    # 這樣全天 24 小時的任何一分鐘，在單位圓上都有唯一且連續的座標
    day_period = 1440.0

    # 透過 2 * pi * (t / 1440)，當時間從 23:59 走到 00:00 時：
    # 角度會極其平滑地從 1.999pi 跨回 0，sin 完美回歸 0，cos 完美回歸 1，不產生任何斷層
    sine_time = np.sin(2 * np.pi * absolute_minutes_of_day / day_period)
    cosine_time = np.cos(2 * np.pi * absolute_minutes_of_day / day_period)

    # --- 輔助特徵：台股日盤的相對分鐘數 (維持盤中核心階段的精準計算) ---
    tws_market_minutes = absolute_minutes_of_day - 540

    # --- 特徵 4 & 5：市場核心階段標籤 (保持原本的 1.0 或 0.0) ---
    is_market_open_30m = np.where((tws_market_minutes >= 0) & (tws_market_minutes <= 30), 1.0, 0.0)
    is_market_close_30m = np.where((tws_market_minutes >= 240) & (tws_market_minutes <= 270), 1.0, 0.0)

    return {
        FeatureName.SIN_TIME: sine_time,
        FeatureName.COS_TIME: cosine_time,
        FeatureName.IS_OP_30: is_market_open_30m,
        FeatureName.IS_CL_30: is_market_close_30m
    }
