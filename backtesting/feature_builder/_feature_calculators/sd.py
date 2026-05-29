import numpy as np


def sd(n, window_seconds, times, closes):
    """45分鐘滾動標準差"""

    result = np.full(n, 0.0, dtype=np.float64)

    cum_c = np.zeros(n + 1, dtype=np.float64)
    cum_c2 = np.zeros(n + 1, dtype=np.float64)
    np.cumsum(closes, out=cum_c[1:])
    np.cumsum(closes ** 2, out=cum_c2[1:])

    for i in range(n):
        lo = times[i] - window_seconds
        left = int(np.searchsorted(times, lo, side='right'))
        right = i + 1
        n = right - left
        if n <= 1:
            result[i] = 0.0
            continue

        mean = (cum_c[right] - cum_c[left]) / n
        mean2 = (cum_c2[right] - cum_c2[left]) / n
        var = mean2 - mean * mean
        result[i] = np.sqrt(var) if var > 0 else 0.0

    return result
