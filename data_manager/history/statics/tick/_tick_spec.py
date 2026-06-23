from datetime import datetime, date, timedelta

from data_manager.history.statics.base._field_base import _FieldBase
from data_manager.history.statics.base._history_data_spec import _HistoryDataSpec
from data_manager.history.statics.base._np_data_base import _NpDataBase
from data_manager.history.statics.tick.np_ticks import NPTicks
from data_manager.history.statics.tick.tick_field import TickField
from database.schema.history_tick import HistoryTick


class TickSpec(_HistoryDataSpec):
    @property
    def field_enum(self) -> type[_FieldBase]:
        return TickField

    @property
    def table_name(self) -> str:
        return HistoryTick.__tablename__

    @property
    def np_data_type(self) -> type[_NpDataBase]:
        return NPTicks

    @property
    def logger_prefix(self) -> str:
        return 'tick'

    def daily_time_range(self, start: date) -> tuple:
        pre_start = start - timedelta(days=1)
        left = datetime(pre_start.year, pre_start.month, pre_start.day, 15, 00, 00)
        right = datetime(start.year, start.month, start.day, 13, 45, 5)
        return left, right


tick_spec = TickSpec()
