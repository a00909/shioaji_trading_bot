from abc import ABC, abstractmethod
from datetime import date, datetime

from data_manager.history.statics.base._field_base import _FieldBase
from data_manager.history.statics.base._np_data_base import _NpDataBase


class _HistoryDataSpec(ABC):

    @property
    @abstractmethod
    def field_enum(self) -> type[_FieldBase]:
        ...

    @property
    @abstractmethod
    def table_name(self) -> str:
        ...

    @property
    @abstractmethod
    def np_data_type(self) -> type[_NpDataBase]:
        ...

    @property
    @abstractmethod
    def logger_prefix(self) -> str:
        ...

    @abstractmethod
    def daily_time_range(self, start: date) -> tuple[datetime, datetime]:
        ...
