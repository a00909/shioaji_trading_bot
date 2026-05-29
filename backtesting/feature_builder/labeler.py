import numpy as np
from numba import njit


@njit
def pnl_label(
        closes,
        times,
        window_seconds,
        use_return=True,
        fill_na=True,
):
    """
    對每個 tick 計算未來 window_seconds 內的 max/min。

    O(n) 實作：forward scan + monotonic deque（array 模擬）。

    Parameters
    ----------
    closes : np.ndarray
        價格陣列
    times : np.ndarray
        秒級 timestamp，需遞增
    window_seconds : float
        未來窗口秒數
    use_return : bool
        True: 回傳報酬率 (future_max / close - 1)
        False: 回傳價差 (future_max - close)
    fill_na : bool
        True: 尾端不足窗口者填 np.nan
        False: 保留 truncated window 結果

    Returns
    -------
    future_max : np.ndarray
    future_min : np.ndarray
    upside : np.ndarray
    downside : np.ndarray
    valid_mask : np.ndarray
    """
    n = len(closes)

    future_max = np.empty(n, dtype=np.float64)
    future_min = np.empty(n, dtype=np.float64)
    upside = np.empty(n, dtype=np.float64)
    downside = np.empty(n, dtype=np.float64)
    valid_mask = np.empty(n, dtype=np.bool_)

    # Array-based monotonic deques (ring buffers)
    max_deque = np.empty(n, dtype=np.int64)
    min_deque = np.empty(n, dtype=np.int64)
    max_head = max_tail = 0
    min_head = min_tail = 0

    right = 0

    for i in range(n):
        window_end = times[i] + window_seconds

        # Expand right boundary to include all ticks <= window_end
        while right < n and times[right] <= window_end:
            # Maintain decreasing max_deque
            while max_head < max_tail and closes[max_deque[max_tail - 1]] <= closes[right]:
                max_tail -= 1
            max_deque[max_tail] = right
            max_tail += 1

            # Maintain increasing min_deque
            while min_head < min_tail and closes[min_deque[min_tail - 1]] >= closes[right]:
                min_tail -= 1
            min_deque[min_tail] = right
            min_tail += 1

            right += 1

        # Remove elements left of i (out of window)
        while max_head < max_tail and max_deque[max_head] < i:
            max_head += 1
        while min_head < min_tail and min_deque[min_head] < i:
            min_head += 1

        future_max[i] = closes[max_deque[max_head]]
        future_min[i] = closes[min_deque[min_head]]

        # Check if there's sufficient future data
        # Precise check: window_end is within the data range
        valid = (times[-1] >= window_end)
        valid_mask[i] = valid

        if use_return:
            upside[i] = future_max[i] / closes[i] - 1.0
            downside[i] = future_min[i] / closes[i] - 1.0
        else:
            upside[i] = future_max[i] - closes[i]
            downside[i] = future_min[i] - closes[i]

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
