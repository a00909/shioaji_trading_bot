from dataclasses import dataclass

import numpy as np

from data_manager.history.statics.base._np_data_base import _NpDataBase


@dataclass
class NPTicks(_NpDataBase):
    """Single day's tick data as numpy arrays."""
    close: np.ndarray  # float64
    volume: np.ndarray  # int64
    tick_type: np.ndarray  # int32
    bid_price: np.ndarray  # float64
    ask_price: np.ndarray  # float64
    bid_volume: np.ndarray  # int64
    ask_volume: np.ndarray  # int64



