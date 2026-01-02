from datetime import datetime


def to_time_key(end: datetime):
    return end.hour * 60 + end.minute