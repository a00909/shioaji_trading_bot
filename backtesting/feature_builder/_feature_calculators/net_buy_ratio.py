import numpy as np
from numba import njit


@njit
def net_buy_ratio(times, tick_types, volumes, window_seconds):
    """
    使用 Numba 與雙指標優化的滾動窗口淨買比率計算
    時間複雜度：O(n)
    空間複雜度：O(1) (除了回傳的 result 陣列外，不佔用額外記憶體)
    """
    n = len(times)
    result = np.zeros(n, dtype=np.float64)

    # 滾動窗口內的動態累加值
    current_buy_vol = 0
    current_sell_vol = 0

    left = 0

    for right in range(n):
        # 1. 將當前右邊界的 Tick 納入窗口
        if tick_types[right] == 1:
            current_buy_vol += volumes[right]
        elif tick_types[right] == 2:
            current_sell_vol += volumes[right]

        # 2. 移動左邊界，將超出 window_seconds 的舊 Tick 剔除
        limit_time = times[right] - window_seconds
        while left < right and times[left] <= limit_time:
            if tick_types[left] == 1:
                current_buy_vol -= volumes[left]
            elif tick_types[left] == 2:
                current_sell_vol -= volumes[left]
            left += 1

        # 3. 計算當前 Tick 的淨買比率
        total_vol = current_buy_vol + current_sell_vol
        if total_vol > 0:
            result[right] = (current_buy_vol - current_sell_vol) / total_vol
        else:
            result[right] = 0.0

    return result


# def net_buy_ratio(n, window_seconds, times, tick_types, volumes):
#     """
#         計算滾動窗口內的淨買比率。
#
#         net_buy_ratio = (active_buy_vol - active_sell_vol) / total_vol
#         > 0: 買方主動（外盤多）
#         < 0: 賣方主動（內盤多）
#     """
#
#     # tick_type == 1: 外盤 = 買方主動
#     # tick_type == 2: 內盤 = 賣方主動
#     active_buy_vol = np.where(tick_types == 1, volumes, 0)
#     active_sell_vol = np.where(tick_types == 2, volumes, 0)
#
#     active_buy_cum = np.zeros(n + 1, dtype=np.int64)
#     active_sell_cum = np.zeros(n + 1, dtype=np.int64)
#     np.cumsum(active_buy_vol, out=active_buy_cum[1:])
#     np.cumsum(active_sell_vol, out=active_sell_cum[1:])
#
#     result = np.full(n, 0.0, dtype=np.float64)
#
#     for i in range(n):
#         hi = times[i]
#         lo = hi - window_seconds
#         left = int(np.searchsorted(times, lo, side='right'))
#         right = i + 1
#
#         bv = active_buy_cum[right] - active_buy_cum[left]
#         sv = active_sell_cum[right] - active_sell_cum[left]
#         total = bv + sv
#         result[i] = (bv - sv) / total if total > 0 else 0.0
#
#     return result
