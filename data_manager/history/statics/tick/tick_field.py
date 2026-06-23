import numpy as np

from data_manager.history.statics.base._field_base import _FieldBase

__all__ = ['TickField']


class TickField(_FieldBase):
    """
    單一事實來源 (Single Source of Truth)
    同時搞定：屬性名、NumPy 型態、struct 格式、二進位長度
    """
    TS = ("ts", np.int64, 'q', 8, '>i8')
    CLOSE = ("close", np.float64, 'd', 8, '>f8')
    VOLUME = ("volume", np.int32, 'i', 4, '>i4')
    TICK_TYPE = ("tick_type", np.int32, 'i', 4, '>i4')
    BID_PRICE = ("bid_price", np.float64, 'd', 8, '>f8')
    ASK_PRICE = ("ask_price", np.float64, 'd', 8, '>f8')
    BID_VOLUME = ("bid_volume", np.int32, 'i', 4, '>i4')
    ASK_VOLUME = ("ask_volume", np.int32, 'i', 4, '>i4')
