from dataclasses import dataclass
from datetime import datetime

import numpy as np

from data_manager.history.statics.base._np_data_base import _NpDataBase


@dataclass
class NPKBars(_NpDataBase):
    close: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    volume: np.ndarray
    amount: np.ndarray

    def __post_init__(self):
        super().__post_init__()
        self._datetime = None

    def datetime_py(self):
        if self._datetime is None:
            self._datetime = super().datetime64().astype('datetime64[ms]').astype(datetime)
        return self._datetime
