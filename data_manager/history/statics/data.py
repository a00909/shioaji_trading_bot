from dataclasses import dataclass
from datetime import date

from shioaji.data import Ticks

from data_manager.history.statics.base._np_data_base import _NpDataBase


@dataclass
class DailyTicks:
    date: date
    ticks: Ticks


@dataclass
class DailySlice[D:_NpDataBase]:
    date: date
    np_slice: D
