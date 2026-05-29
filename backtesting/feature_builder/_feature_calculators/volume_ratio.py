from datetime import datetime

import numpy as np


def rolling_vol_sum_window(n, window_seconds, times, volumes):
    """滾動窗口內的成交量總和（O(n) 累積和技巧）"""

    result = np.full(n, 0.0, dtype=np.float64)
    cum_vol = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(volumes, out=cum_vol[1:])

    for i in range(n):
        hi = times[i]
        lo = hi - window_seconds
        left = int(np.searchsorted(times, lo, side='right'))
        right = i + 1
        result[i] = cum_vol[right] - cum_vol[left]

    return result


def volume_ratio(n, window_seconds, times, volumes, iiva_lookup_fn):
    """
            量比 = 滾動窗口內成交量總和 / IIVA（歷史同期均量）

            若無 iiva_lookup，回傳 1.0
            """

    # rolling_vol_sum：過去5分鐘內所有tick的成交量總和
    rolling_vol_sum = rolling_vol_sum_window(
        n, window_seconds, times, volumes
    )

    # 建立 IIVA 查詢表
    iiva_array = np.full(n, 1.0, dtype=np.float64)
    for i in range(n):
        ts = datetime.fromtimestamp(times[i])
        aligned = ts.replace(second=0, microsecond=0)
        iiva_array[i] = iiva_lookup_fn(aligned)

    result = np.where(
        iiva_array > 0,
        rolling_vol_sum / iiva_array,
        1.0
    )

    return result
