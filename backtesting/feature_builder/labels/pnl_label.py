import numpy as np
import pandas as pd
from numba import njit

@njit
def pnl_label( # gemini ver.
        closes,
        times,
        window_seconds,
        use_return=True,
        fill_na=True,
):
    n = len(closes)

    future_max = np.empty(n, dtype=np.float64)
    future_min = np.empty(n, dtype=np.float64)
    upside = np.empty(n, dtype=np.float64)
    downside = np.empty(n, dtype=np.float64)
    valid_mask = np.empty(n, dtype=np.bool_)

    max_deque = np.empty(n, dtype=np.int64)
    min_deque = np.empty(n, dtype=np.int64)
    max_head = max_tail = 0
    min_head = min_tail = 0

    # 修正點 1：右邊掃描指針改從 1 開始，配合主迴圈，確保絕對不包含當前第 i 個 tick
    right = 1

    for i in range(n):
        # 如果走到最後一個 tick，後面沒數據了，直接填 NaN
        if i == n - 1:
            valid_mask[i] = False
            future_max[i] = np.nan; future_min[i] = np.nan
            upside[i] = np.nan; downside[i] = np.nan
            continue

        window_start = times[i + 1]  # 未來的起算時間點
        window_end = window_start + window_seconds

        # 確保 right 指針至少從 i + 1 開始
        if right <= i:
            right = i + 1

        # Expand right boundary: 只塞入從 i+1 開始的未來 tick
        while right < n and times[right] <= window_end:
            while max_head < max_tail and closes[max_deque[max_tail - 1]] <= closes[right]:
                max_tail -= 1
            max_deque[max_tail] = right
            max_tail += 1

            while min_head < min_tail and closes[min_deque[min_tail - 1]] >= closes[right]:
                min_tail -= 1
            min_deque[min_tail] = right
            min_tail += 1

            right += 1

        # Remove elements out of window (必須大於等於 i + 1)
        while max_head < max_tail and max_deque[max_head] < i + 1:
            max_head += 1
        while min_head < min_tail and min_deque[min_head] < i + 1:
            min_head += 1

        # 修正點 2：安全檢查，如果窗口內沒數據（例如此時橫盤時間戳有跳躍）
        if max_head == max_tail:
            future_max[i] = closes[i + 1]
            future_min[i] = closes[i + 1]
        else:
            future_max[i] = closes[max_deque[max_head]]
            future_min[i] = closes[min_deque[min_head]]

        valid = (times[-1] >= window_end)
        valid_mask[i] = valid

        # 修正點 3：終極防禦！計算價差與報酬率時，
        # 基位價格（減數/分母）嚴格改用 closes[i + 1]（即你下一筆才能成交的預期價格）
        # 這樣一來，closes[i] 的穿越橋樑被徹底砸碎！
        base_price = closes[i + 1]

        if use_return:
            upside[i] = future_max[i] / base_price - 1.0
            downside[i] = future_min[i] / base_price - 1.0
        else:
            upside[i] = future_max[i] - base_price
            downside[i] = future_min[i] - base_price

        if fill_na and not valid:
            future_max[i] = np.nan
            future_min[i] = np.nan
            upside[i] = np.nan
            downside[i] = np.nan

    return (
        future_max,
        future_min,
        upside,
        downside,
        valid_mask,
    )




# @njit
# def pnl_label(
#         closes,
#         times,
#         window_seconds,
#         use_return=True,
#         fill_na=True,
# ):
#     """
#     對每個 tick 計算未來 window_seconds 內的 max/min。
#
#     O(n) 實作：forward scan + monotonic deque（array 模擬）。
#
#     Parameters
#     ----------
#     closes : np.ndarray
#         價格陣列
#     times : np.ndarray
#         秒級 timestamp，需遞增
#     window_seconds : float
#         未來窗口秒數
#     use_return : bool
#         True: 回傳報酬率 (future_max / close - 1)
#         False: 回傳價差 (future_max - close)
#     fill_na : bool
#         True: 尾端不足窗口者填 np.nan
#         False: 保留 truncated window 結果
#
#     Returns
#     -------
#     future_max : np.ndarray
#     future_min : np.ndarray
#     upside : np.ndarray
#     downside : np.ndarray
#     valid_mask : np.ndarray
#     """
#     n = len(closes)
#
#     future_max = np.empty(n, dtype=np.float64)
#     future_min = np.empty(n, dtype=np.float64)
#     upside = np.empty(n, dtype=np.float64)
#     downside = np.empty(n, dtype=np.float64)
#     valid_mask = np.empty(n, dtype=np.bool_)
#
#     # Array-based monotonic deques (ring buffers)
#     max_deque = np.empty(n, dtype=np.int64)
#     min_deque = np.empty(n, dtype=np.int64)
#     max_head = max_tail = 0
#     min_head = min_tail = 0
#
#     right = 0
#
#     for i in range(n):
#         window_end = times[i] + window_seconds
#
#         # Expand right boundary to include all ticks <= window_end
#         while right < n and times[right] <= window_end:
#             # Maintain decreasing max_deque
#             while max_head < max_tail and closes[max_deque[max_tail - 1]] <= closes[right]:
#                 max_tail -= 1
#             max_deque[max_tail] = right
#             max_tail += 1
#
#             # Maintain increasing min_deque
#             while min_head < min_tail and closes[min_deque[min_tail - 1]] >= closes[right]:
#                 min_tail -= 1
#             min_deque[min_tail] = right
#             min_tail += 1
#
#             right += 1
#
#         # Remove elements left of i (out of window)
#         while max_head < max_tail and max_deque[max_head] < i:
#             max_head += 1
#         while min_head < min_tail and min_deque[min_head] < i:
#             min_head += 1
#
#         future_max[i] = closes[max_deque[max_head]]
#         future_min[i] = closes[min_deque[min_head]]
#
#         # Check if there's sufficient future data
#         # Precise check: window_end is within the data range
#         valid = (times[-1] >= window_end)
#         valid_mask[i] = valid
#
#         if use_return:
#             upside[i] = future_max[i] / closes[i] - 1.0
#             downside[i] = future_min[i] / closes[i] - 1.0
#         else:
#             upside[i] = future_max[i] - closes[i]
#             downside[i] = future_min[i] - closes[i]
#
#         if fill_na and not valid:
#             future_max[i] = np.nan
#             future_min[i] = np.nan
#             upside[i] = np.nan
#             downside[i] = np.nan
#
#     return (
#         future_max,
#         future_min,
#         upside,
#         downside,
#         valid_mask,
#     )


def combine_label(fav_arr, adv_arr, threshold=0.0005):
    # 1. 取得絕對值較大的那一方的索引（True 表示 fav 絕對值大，False 表示 adv 絕對值大）
    fav_win = np.abs(fav_arr) >= np.abs(adv_arr)

    # 2. 根據比較結果，挑選出保留原正負號的值
    max_by_abs = np.where(fav_win, fav_arr, adv_arr)

    # 3. 只有當「絕對值的最大值」大於門檻時才寫入，否則為 0.0
    # （注意：這裡的 threshold 應該也是正數指標）
    label = np.where(np.abs(max_by_abs) > threshold, max_by_abs, 0.0)


    df_box = pd.Series(label)

    # 直接印出描述性統計
    print('label 分布:')
    print(df_box.describe())

    return label
