from abc import ABC, abstractmethod
from dataclasses import dataclass, fields

from datetime import datetime
from decimal import Decimal

from mixins.datetime_comparable_mixin import DatetimeComparableMixin
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import decode_redis


@dataclass(frozen=True, slots=True)
class Base(DatetimeComparableMixin, ABC):
    code: str = ''
    datetime: datetime = None
    simtrade: bool = False

    DESERIALIZE_CASTERS = {
        bool: lambda v: bool(int(v)),
        datetime: lambda v: datetime.fromtimestamp(float(v), tz=DEFAULT_TIMEZONE),
        list[int]: lambda v: [int(_v) for _v in v.split(',')],
        list[Decimal]: lambda v: [Decimal(_v) for _v in v.split(',')],
    }

    SERIALIZE_CASTERS = {
        bool: lambda v: str(1 if v else 0),
        datetime: lambda v: str(v.timestamp()),
        list[int]: lambda v: ','.join([str(_v) for _v in v]),
        list[Decimal]: lambda v: ','.join([str(_v) for _v in v]),
    }

    def __post_init__(self):
        if type(self) is Base:
            self._cant_be_init_error()

    def _cant_be_init_error(self):
        raise TypeError(f"{self.__class__.__name__} can not be instantiated, because it is not completed.")

    @property
    @abstractmethod
    def _corresponding_sj_type(self):
        raise NotImplementedError

    @classmethod
    def from_sj(cls, sj_data):
        if not type(sj_data) == cls._corresponding_sj_type:
            raise TypeError(f"required {cls._corresponding_sj_type}, but: {type(sj_data)}")
        kwargs = {}

        for f in fields(cls):
            kwargs[f.name] = getattr(sj_data, f.name)

        return cls(**kwargs)

    def serialize(self, serial_num=-1, separator=':'):
        values = [str(serial_num)]  # 避免redis key重複

        for field in fields(self):
            v = getattr(self, field.name)
            caster = self.SERIALIZE_CASTERS.get(field.type, str)
            values.append(caster(v))

        return separator.join(values)

    @classmethod
    def deserialize(cls, data: bytes, separator=":"):
        parts = decode_redis(data).split(separator)
        kwargs = {}

        for f, raw in zip(fields(cls), parts[1:]):  # skip one for serial num
            caster = cls.DESERIALIZE_CASTERS.get(f.type, f.type)
            kwargs[f.name] = caster(raw)

        return cls(**kwargs)
