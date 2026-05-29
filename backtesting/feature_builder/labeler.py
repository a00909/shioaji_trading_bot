import numpy as np
from numba import njit

@njit
def pnl_label(closes,times,lookahead_sec):
    """
       計算未來 N 分鐘內的最大浮盈/浮虧。

       :param closes: 收盤價序列
       :param times: 時間戳序列（秒）
       :param lookahead_sec: 前瞻視窗（秒）
       :return: (max_favorable, max_adverse)
       """
    n = len(closes)
    max_fav = np.zeros(n)
    max_adv = np.zeros(n)

    right = n - 1
    for i in range(n - 2, -1, -1):
        window_end = times[i] + lookahead_sec

        # 收縮右界
        while right > i and times[right] > window_end:
            right -= 1

        if right <= i:
            continue  # 沒有未來資料

        # 在 [i+1, right] 找極值
        best_hi = closes[i + 1]
        best_lo = closes[i + 1]
        for j in range(i + 2, right + 1):
            if closes[j] > best_hi:
                best_hi = closes[j]
            if closes[j] < best_lo:
                best_lo = closes[j]

        max_fav[i] = best_hi - closes[i]
        max_adv[i] = best_lo - closes[i]

    return max_fav, max_adv