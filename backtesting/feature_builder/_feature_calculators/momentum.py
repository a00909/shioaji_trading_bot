import numpy as np


def momentum(n,window_seconds,times,closes):
    """
           計算價格動量（相對變化率）。

           momentum = (current_price - price_N_minutes_ago) / price_N_minutes_ago
           """

    result = np.full(n, 0.0, dtype=np.float64)

    for i in range(n):
        hi = times[i]
        lo = hi - window_seconds
        left = int(np.searchsorted(times, lo, side='right'))

        if left < i:
            past_price = closes[left]
            if past_price > 0:
                result[i] = (closes[i] - past_price) / past_price
            else:
                result[i] = 0.0

    return result