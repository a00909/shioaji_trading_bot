from datetime import timedelta, datetime
from typing import Any

from mixins.datetime_comparable_mixin import DatetimeComparableMixin
from strategy.tools.indicator_provider.extensions.data.extensions.indicator_type import IndicatorType
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import decode_redis


class Indicator(DatetimeComparableMixin):
    L1_SEPERATOR = ':'

    def __init__(self):
        super().__init__()
        self.indicator_type: IndicatorType = None
        self.length: timedelta = None

        self.data_count: int = None
        self.value: float | Any = None
        self.datetime: datetime = None

    @classmethod
    def deserialize(cls, data: str | bytes, from_subclass=False, subclass_instance=None):
        if not from_subclass:
            data = decode_redis(data)
            instance = cls()
        else:
            instance = subclass_instance

        values = data.split(cls.L1_SEPERATOR)
        instance.indicator_type = IndicatorType.from_string(values[1].split('.')[1])
        instance.data_count = int(values[2])
        instance.value = float(values[3])
        instance.datetime = datetime.fromtimestamp(float(values[4]), tz=DEFAULT_TIMEZONE)

        return instance if not from_subclass else None

    def serialize(self, serial):
        data = (
            f'{serial}{self.L1_SEPERATOR}'
            f'{self.indicator_type}{self.L1_SEPERATOR}'
            f'{self.data_count}{self.L1_SEPERATOR}'
            f'{self.value}{self.L1_SEPERATOR}'
            f'{self.datetime.timestamp()}'

        )
        return data

    def get(self):
        if not self.value:
            self.value = self._calc()
        return self.value

    def _calc(self):
        """
        child class should override this method if not use self.value
        :return:
        """
        raise NotImplemented
