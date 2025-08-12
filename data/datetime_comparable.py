from abc import ABC
from datetime import datetime


class DatetimeComparable(ABC):
    def __init__(self, compare_class):
        self.compare_class = compare_class

    def __lt__(self, other):
        if isinstance(other, datetime):
            return self.datetime < other
        elif isinstance(other, self.compare_class):
            return self.datetime < other.datetime
        return NotImplemented

    def __eq__(self, other):
        if isinstance(other, datetime):
            return self.datetime == other
        elif isinstance(other, self.compare_class):
            return self.datetime == other.datetime
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, datetime):
            return self.datetime > other
        elif isinstance(other, self.compare_class):
            return self.datetime > other.datetime
        return NotImplemented
