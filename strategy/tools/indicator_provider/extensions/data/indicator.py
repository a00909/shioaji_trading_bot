from datetime import timedelta, datetime

from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import decode_redis


class Indicator:
    def __init__(self):
        self.indicator_type: [IndicatorType] = None
        self.length: timedelta = None

        self.data_count: int = None
        self.value: float = None
        self.datetime: datetime = None

    @classmethod
    def deserialize(cls, data: bytes, separator=':'):
        values = decode_redis(data).split(separator)
        indicator = cls()
        indicator.indicator_type = IndicatorType.from_string(values[0].split('.')[1])
        indicator.data_count = int(values[1])
        indicator.value = float(values[2])
        indicator.datetime = datetime.fromtimestamp(float(values[3]), tz=DEFAULT_TIMEZONE)
        return indicator

    def serialize(self, serial, seperator=':'):
        data = (
            f'{self.indicator_type}{seperator}'
            f'{self.data_count}{seperator}'
            f'{self.value}{seperator}'
            f'{self.datetime.timestamp()}{seperator}'
            f'{serial}'
        )
        return data
