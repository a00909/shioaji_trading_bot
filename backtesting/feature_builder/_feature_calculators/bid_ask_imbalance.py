import numpy as np
from numba import njit


@njit
def bid_ask_features(n, bid_v, ask_v, bid_p, ask_p, times, window_seconds):
    # 存放結果
    imb_windowed = np.zeros(n, dtype=np.float64)
    mid_windowed = np.zeros(n, dtype=np.float64)
    spread_windowed = np.zeros(n, dtype=np.float64)

    # 累加變數
    sum_bid_v = 0.0
    sum_ask_v = 0.0
    sum_mid = 0.0
    sum_spread = 0.0

    start = 0
    for i in range(n):
        # 加入當前 Tick 資料
        sum_bid_v += float(bid_v[i])
        sum_ask_v += float(ask_v[i])
        sum_mid += (float(bid_p[i]) + float(ask_p[i])) / 2.0
        sum_spread += (float(ask_p[i]) - float(bid_p[i]))

        # 核心邏輯：向後壓縮窗口，移除所有超過 window_seconds 的舊資料
        while times[i] - times[start] >= window_seconds:
            sum_bid_v -= float(bid_v[start])
            sum_ask_v -= float(ask_v[start])
            sum_mid -= (float(bid_p[start]) + float(ask_p[start])) / 2.0
            sum_spread -= (float(ask_p[start]) - float(bid_p[start]))
            start += 1

        # 計算統計量 (使用窗口內實際包含的 Tick 數量)
        count = float(i - start + 1)

        total_vol = sum_bid_v + sum_ask_v
        if total_vol > 0:
            imb_windowed[i] = (sum_bid_v - sum_ask_v) / total_vol

        mid_windowed[i] = sum_mid / count
        spread_windowed[i] = sum_spread / count

    return imb_windowed, mid_windowed, spread_windowed


@njit
def compute_imb_change_rate(n, timestamps, ba_imb, delta_seconds=5):
    """
    計算 ba_imb 相對於過去 N 秒的變化率 (速度)

    n: 數據總長度
    timestamps: 時間戳陣列 (單位：秒)
    ba_imb: 盤口不平衡度陣列 (你剛算出來的)
    delta_seconds: 回溯的時間窗口 (例如 5 秒或 10 秒)
    """
    imb_velocity = np.zeros(n, dtype=np.float64)

    start = 0
    for i in range(n):
        # 核心邏輯：建立一個恰好落後 delta_seconds 的左邊界指標 (start)
        while timestamps[i] - timestamps[start] > delta_seconds:
            # 這裡只移動指標，不增減累加值，目的是找到「N秒前的那個時間點」
            if start + 1 < i and timestamps[i] - timestamps[start + 1] >= delta_seconds:
                start += 1
            else:
                break

        # 變化率 = 當前值 - N秒前的值
        # (這裡使用絕對差值；因為 ba_imb 本身就在 -1 ~ 1 之間，用相減最穩定，避免除以接近 0 的數字導致溢位)
        imb_velocity[i] = ba_imb[i] - ba_imb[start]

    return imb_velocity