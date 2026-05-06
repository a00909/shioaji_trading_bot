from datetime import timedelta, date
from functools import lru_cache


def group_dates_into_ranges(dates: list[date]) -> list[tuple[date, date]]:
    if not dates:
        return []

    sorted_dates = sorted(dates)
    ranges = []
    range_start = sorted_dates[0]
    prev_date = sorted_dates[0]

    for current in sorted_dates[1:]:
        if current == prev_date + timedelta(days=1):
            prev_date = current
        else:
            ranges.append((range_start, prev_date))
            range_start = current
            prev_date = current

    ranges.append((range_start, prev_date))
    return ranges


def subtract_ranges(bigger: list[tuple[date, date]], smaller: list[tuple[date, date]]) -> list[tuple[date, date]]:
    """
    Compute A - B where A and B are sorted, non-overlapping ranges (inclusive).
    Robust: handles B ranges that may span across multiple A ranges or lie outside A.
    Returns a list of (start_date, end_date) tuples (inclusive).
    Time complexity: O(len(A) + len(B)).
    """
    result = []
    # make a mutable copy of B so we can update start when B spans multiple A's
    b_list = [[b_start, b_end] for (b_start, b_end) in smaller]
    j = 0

    for a_start, a_end in bigger:
        cur_start = a_start

        # skip B's that end before current A's start
        while j < len(b_list) and b_list[j][1] < cur_start:
            j += 1

        while j < len(b_list):
            b_start, b_end = b_list[j]

            # if the next B starts after current A ends, done with this A
            if b_start > a_end:
                break

            # keep the gap before B within current A
            if cur_start < b_start:
                result.append((cur_start, b_start - timedelta(days=1)))

            # consume the intersection of B with current A
            if b_end <= a_end:
                # B finishes inside current A -> move cur_start after B and consume B entirely
                cur_start = b_end + timedelta(days=1)
                j += 1
            else:
                # B extends beyond current A -> update B's start to the first day after current A
                # (so the remaining part of B will be applied to subsequent A segments)
                b_list[j][0] = a_end + timedelta(days=1)
                cur_start = b_end + timedelta(days=1)  # this will be > a_end, so loop exits
                break

        # leftover tail of A
        if cur_start <= a_end:
            result.append((cur_start, a_end))

    return result


@lru_cache(maxsize=128)
def enumerate_dates_set_by_range(start: date, end: date):
    # 使用 frozenset 確保快取結果不可被外部修改
    all_dates = frozenset(
        start + timedelta(days=i)
        for i in range((end - start).days + 1)
    )
    return all_dates
