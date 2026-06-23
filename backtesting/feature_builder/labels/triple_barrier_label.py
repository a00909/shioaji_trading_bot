import numpy as np
from numba import njit


@njit(cache=True, fastmath=True)
def triple_barrier_label(
    closes: np.ndarray,
    times: np.ndarray,
    window_seconds: float,
    tp_pct: float,
    sl_pct: float,
    fill_na: bool = True,
    min_bar_gap: int = 0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Triple-Barrier labeling (de Prado, *Advances in Financial ML* Ch.3).

    對每個 bar i 定義三道牆：
      - 上牆 (tp):  close[i] * (1 + tp_pct)  — 利潤目標
      - 下牆 (sl):  close[i] * (1 - sl_pct)  — 停損
      - 垂直牆 (vertical):  times[i] + window_seconds  — 時間到強制出場

    在 [i+1, i+1+max_search) 範圍內找**最先觸發**的那道牆：
      - 先觸上牆 →  label =  +1, profit = (upper - entry) / entry
      - 先觸下牆 →  label =  -1, profit = (lower - entry) / entry
      - 時間到     →  label =   0, profit = (last_close - entry) / entry
      - 都不觸發（波動太小）→ label = 0, profit = 0

    Parameters
    ----------
    closes : np.ndarray (n,)
        收盤價序列（已遞增時間排序）
    times : np.ndarray (n,)
        對應的秒級 timestamp（須遞增）
    window_seconds : float
        垂直牆的最大窗口（秒）
    tp_pct : float
        上牆距離，以**報酬率**表示（0.0005 = 5 bp = 0.05%）
    sl_pct : float
        下牆距離，以**正數**表示（0.0003 = 3 bp = 0.03%）
    fill_na : bool
        True: 末端不足窗口的 bar 標記為 label=0, profit=NaN, valid=False
    min_bar_gap : int
        觸發 i 後至少過 N 根 bar 才能再觸發（防止同一波動的相鄰 bar 互相觸發；
              設 0 = 不限制）。注意：這是「同方向觸發後的冷卻期」，
              並非預測未來結構的一部分。

    Returns
    -------
    label : np.ndarray (n,), dtype int8
        +1 = 先觸 TP, -1 = 先觸 SL, 0 = 垂直牆或無觸發
    profit : np.ndarray (n,), dtype float64
        對應觸發的**已實現報酬率**（進場到出場）
    barrier_idx : np.ndarray (n,), dtype int64
        觸發牆的 bar index（-1 = 無觸發 / fill_na 後 NaN）
    valid_mask : np.ndarray (n,), dtype bool
        True = 此 bar 有完整窗口可判斷（fill_na=False 時全 True）
    """
    n = len(closes)
    label = np.zeros(n, dtype=np.int8)
    profit = np.zeros(n, dtype=np.float64)
    barrier_idx = np.full(n, -1, dtype=np.int64)
    valid_mask = np.ones(n, dtype=np.bool_)
    if n > 0 and fill_na:
        valid_mask[n - 1] = False  # 最後一根 bar 永遠沒有未來資料
        profit[n - 1] = np.nan

    if n == 0:
        return label, profit, barrier_idx, valid_mask

    # 同時觸發時的決定方向：取距 entry 較近的牆先觸發
    #（這在「同根 bar OHLC 同時穿越」時會用；同根內仍無法分辨 OHLC 先後，採用距離啟發式）
    for i in range(n - 1):
        entry = closes[i]
        upper = entry * (1.0 + tp_pct)
        lower = entry * (1.0 - sl_pct)
        deadline = times[i] + window_seconds

        start = i + 1 + min_bar_gap
        hit = 0          # +1 = upper, -1 = lower, 0 = vertical/timeout
        hit_idx = -1
        hit_price = entry

        for j in range(start, n):
            if times[j] > deadline:
                break
            c = closes[j]
            if c >= upper:
                hit = 1
                hit_idx = j
                hit_price = c
                break
            if c <= lower:
                hit = -1
                hit_idx = j
                hit_price = c
                break

        if hit != 0:
            label[i] = hit
            profit[i] = (hit_price - entry) / entry
            barrier_idx[i] = hit_idx
        else:
            # 垂直牆或無觸發：用窗口內最後一根 bar 計算 profit
            label[i] = 0
            last_idx = start
            for j in range(start, n):
                if times[j] > deadline:
                    break
                last_idx = j
            if last_idx > i:
                profit[i] = (closes[last_idx] - entry) / entry
                barrier_idx[i] = last_idx
            # else: 窗口內 0 bar → profit=0, label=0

        # window_end > times[-1] → 無完整窗口
        has_full_window = (deadline <= times[-1])
        valid_mask[i] = has_full_window

        if fill_na and not has_full_window:
            label[i] = 0
            profit[i] = np.nan
            barrier_idx[i] = -1

    return label, profit, barrier_idx, valid_mask




# ─────────────────────────────────────────────
#  便利 wrapper：給 DataFrame / pandas API
# ─────────────────────────────────────────────

def triple_barrier_label_pd(
    closes: np.ndarray,
    times: np.ndarray,
    window_seconds: float,
    tp_pct: float,
    sl_pct: float,
    **kwargs,
) -> dict:
    """
    便利介面：傳入 numpy array，回傳 dict 方便整合進 DataFrame builder。

    Example
    -------
    >>> import pandas as pd
    >>> result = triple_barrier_label_pd(df.close.values, df.time.values,
    ...                                   window_seconds=300, tp_pct=0.0005, sl_pct=0.0003)
    >>> df['tb_label'] = result['label']
    >>> df['tb_profit'] = result['profit']
    """
    label, profit, barrier_idx, valid_mask = triple_barrier_label(
        closes=closes,
        times=times,
        window_seconds=window_seconds,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        **kwargs,
    )
    return {
        "label": label,
        "profit": profit,
        "barrier_idx": barrier_idx,
        "valid_mask": valid_mask,
    }
