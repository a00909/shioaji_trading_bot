from datetime import time, datetime


def total_minutes(t: datetime | time) -> int:
    return t.hour * 60 + t.minute


def minute_index(t: datetime | time, interval_min: int) -> int:
    return total_minutes(t) // interval_min


def align_minute(ts: datetime | time, interval_min: int) -> int:
    """
    把時間對齊到 interval 分鐘
    例如 align_time(2025-08-31 09:01, 5) -> 2025-08-31 09:00
    """
    m = total_minutes(ts)
    aligned_minutes = m - m % interval_min
    return aligned_minutes
