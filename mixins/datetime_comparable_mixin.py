from datetime import datetime
from functools import total_ordering


# todo: 比照看看ts_comparable_mixin看有沒有辦法簡化
@total_ordering
class DatetimeComparableMixin:
    _datetime_comparable_field_name = 'datetime'

    def _get_datetime_field(self):
        return getattr(self, self._datetime_comparable_field_name)

    def __lt__(self, other):
        field = self._get_datetime_field()
        if isinstance(other, datetime):
            return field < other
        elif isinstance(other, self.__class__):
            return field < other._get_datetime_field()
        return NotImplemented

    def __eq__(self, other):
        field = self._get_datetime_field()
        if isinstance(other, datetime):
            return field == other
        elif isinstance(other, self.__class__):
            return field == other._get_datetime_field()
        return NotImplemented

    def __gt__(self, other):
        field = self._get_datetime_field()
        if isinstance(other, datetime):
            return field > other
        elif isinstance(other, self.__class__):
            return field > other._get_datetime_field()
        return NotImplemented
