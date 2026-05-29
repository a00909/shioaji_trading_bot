import numpy as np


def net_buy_ratio(n, window_seconds, times, tick_types, volumes):
    """
        計算滾動窗口內的淨買比率。

        net_buy_ratio = (active_buy_vol - active_sell_vol) / total_vol
        > 0: 買方主動（外盤多）
        < 0: 賣方主動（內盤多）
    """

    # tick_type == 1: 外盤 = 買方主動
    # tick_type == 2: 內盤 = 賣方主動
    active_buy_vol = np.where(tick_types == 1, volumes, 0)
    active_sell_vol = np.where(tick_types == 2, volumes, 0)

    active_buy_cum = np.zeros(n + 1, dtype=np.int64)
    active_sell_cum = np.zeros(n + 1, dtype=np.int64)
    np.cumsum(active_buy_vol, out=active_buy_cum[1:])
    np.cumsum(active_sell_vol, out=active_sell_cum[1:])

    result = np.full(n, 0.0, dtype=np.float64)

    for i in range(n):
        hi = times[i]
        lo = hi - window_seconds
        left = int(np.searchsorted(times, lo, side='right'))
        right = i + 1

        bv = active_buy_cum[right] - active_buy_cum[left]
        sv = active_sell_cum[right] - active_sell_cum[left]
        total = bv + sv
        result[i] = (bv - sv) / total if total > 0 else 0.0

    return result
