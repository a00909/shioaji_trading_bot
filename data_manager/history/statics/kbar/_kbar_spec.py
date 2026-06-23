from datetime import datetime, date, timedelta

from data_manager.history.statics.base._field_base import _FieldBase
from data_manager.history.statics.base._history_data_spec import _HistoryDataSpec
from data_manager.history.statics.base._np_data_base import _NpDataBase
from data_manager.history.statics.kbar.kbar_field import KBarField
from data_manager.history.statics.kbar.np_kbars import NPKBars
from database.schema.kbar import KBar


class KBarSpec(_HistoryDataSpec):

    @property
    def field_enum(self) -> type[_FieldBase]:
        return KBarField

    @property
    def table_name(self) -> str:
        return KBar.__tablename__

    @property
    def np_data_type(self) -> type[_NpDataBase]:
        return NPKBars

    @property
    def logger_prefix(self) -> str:
        return 'kbar'

    def daily_time_range(self, start: date) -> tuple:
        post_start = start + timedelta(days=1)
        left = datetime(start.year, start.month, start.day, 00, 00, 00)
        right = datetime(post_start.year, post_start.month, post_start.day, 00, 00, 00)
        return left, right


kbar_spec = KBarSpec()
