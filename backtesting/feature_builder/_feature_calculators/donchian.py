import numpy as np
from numba import njit


@njit
def donchian(n, seconds, prices, times):
    # 原本的輸出陣列
    h_arr = np.zeros(n, dtype=np.float64)
    l_arr = np.zeros(n, dtype=np.float64)
    ha_arr = np.zeros(n, dtype=np.float64)
    la_arr = np.zeros(n, dtype=np.float64)
    dir_arr = np.zeros(n, dtype=np.float64)

    # 新增的歸一化輸出陣列 (降維特徵)
    ha_pct_arr = np.zeros(n, dtype=np.float64)
    la_pct_arr = np.zeros(n, dtype=np.float64)

    h_q = np.empty(n, dtype=np.int64)
    l_q = np.empty(n, dtype=np.int64)
    h_head = h_tail = 0
    l_head = l_tail = 0

    ha = la = 0
    direction = 0
    left_ptr = 0

    for i in range(n):
        ts = times[i]
        price = prices[i]
        window_left = ts - seconds

        while h_head < h_tail and times[h_q[h_head]] < window_left: h_head += 1
        while l_head < l_tail and times[l_q[l_head]] < window_left: l_head += 1
        while left_ptr < i and times[left_ptr] < window_left: left_ptr += 1

        h = prices[h_q[h_head]] if h_head < h_tail else price
        l = prices[l_q[l_head]] if l_head < l_tail else price

        if price > h:
            if la == 0:
                ha += 1
            else:
                ha, la = 1, 0
            direction = 1
        elif price < l:
            if ha == 0:
                la += 1
            else:
                la, ha = 1, 0
            direction = -1

        while h_head < h_tail and prices[h_q[h_tail - 1]] <= price: h_tail -= 1
        h_q[h_tail] = i;
        h_tail += 1
        while l_head < l_tail and prices[l_q[l_tail - 1]] >= price: l_tail -= 1
        l_q[l_tail] = i;
        l_tail += 1

        h_arr[i] = h
        l_arr[i] = l
        ha_arr[i] = ha
        la_arr[i] = la
        dir_arr[i] = direction

        # 降維邏輯
        window_tick_count = float(i - left_ptr + 1)
        ha_pct_arr[i] = ha / window_tick_count
        la_pct_arr[i] = la / window_tick_count

    return ha_pct_arr, la_pct_arr, dir_arr, h_arr, l_arr,


# @njit
# def donchian(n, seconds, prices, times):
#     """
#     單調雙端佇列實作滾動 Donchian Channel + ha/la 累計。
#
#     規則：
#       price > h 且 la == 0 → ha += 1, h = price
#       price > h 且 la != 0 → ha = 1, la = 0, h = price
#       price < l 且 ha == 0 → la += 1, l = price
#       price < l 且 ha != 0 → la = 1, ha = 0, l = price
#
#     視窗過期時，h/l 由單調 deque 動態維護。
#     時間複雜度：O(n)，每個元素最多進 deque 一次。
#     """
#
#     h_arr = np.zeros(n, dtype=np.float64)
#     l_arr = np.zeros(n, dtype=np.float64)
#     ha_arr = np.zeros(n, dtype=np.float64)
#     la_arr = np.zeros(n, dtype=np.float64)
#     dir_arr = np.zeros(n, dtype=np.float64)
#
#     # Array-based monotonic deques (store indices)
#     h_q = np.empty(n, dtype=np.int64)  # 遞減，前端 = 視窗 max
#     l_q = np.empty(n, dtype=np.int64)  # 遞增，前端 = 視窗 min
#     h_head = h_tail = 0
#     l_head = l_tail = 0
#
#     ha = la = 0
#     direction = 0  # 1=up, -1=down, 0=neutral
#
#     for i in range(n):
#         ts = times[i]
#         price = prices[i]
#         window_left = ts - seconds
#
#         # 1. 移除過期元素（從前端）
#         while h_head < h_tail and times[h_q[h_head]] < window_left:
#             h_head += 1
#         while l_head < l_tail and times[l_q[l_head]] < window_left:
#             l_head += 1
#
#         # 2. 取出視窗 max / min（插入新 tick 之前的值）
#         if h_head < h_tail:
#             h = prices[h_q[h_head]]
#         else:
#             h = price  # deque 為空，設為當前價格
#
#         if l_head < l_tail:
#             l = prices[l_q[l_head]]
#         else:
#             l = price  # deque 為空，設為當前價格
#
#         # 3. 套用 4 條規則更新 ha / la / dir（比較的是舊的 h/l）
#         if price > h:
#             if la == 0:
#                 ha += 1
#             else:
#                 ha = 1
#                 la = 0
#             direction = 1
#         elif price < l:
#             if ha == 0:
#                 la += 1
#             else:
#                 la = 1
#                 ha = 0
#             direction = -1
#         # else: 區間內，不變
#
#         # 4. 維護單調性 + 插入新元素
#         # h_q（遞減）：新値比後端大就 pop 後端
#         while h_head < h_tail and prices[h_q[h_tail - 1]] <= price:
#             h_tail -= 1
#         h_q[h_tail] = i
#         h_tail += 1
#
#         # l_q（遞增）：新値比後端小就 pop 後端
#         while l_head < l_tail and prices[l_q[l_tail - 1]] >= price:
#             l_tail -= 1
#         l_q[l_tail] = i
#         l_tail += 1
#
#         h_arr[i] = h
#         l_arr[i] = l
#         ha_arr[i] = ha
#         la_arr[i] = la
#         dir_arr[i] = direction
#
#     return ha_arr, la_arr, dir_arr, h_arr, l_arr
