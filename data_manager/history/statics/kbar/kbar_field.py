import numpy as np

from data_manager.history.statics.base._field_base import _FieldBase

__all__ = ['KBarField']


class KBarField(_FieldBase):
    """
    單一事實來源 (Single Source of Truth)
    同時搞定：屬性名、NumPy 型態、struct 格式、二進位長度
    """
    TS = ("ts", np.int64, 'q', 8, '>i8')
    CLOSE = ("close", np.float64, 'd', 8, '>f8')
    OPEN = ("open", np.float64, 'd', 8, '>f8')
    HIGH = ("high", np.float64, 'd', 8, '>f8')
    LOW = ("low", np.float64, 'd', 8, '>f8')
    VOLUME = ("volume", np.int32, 'i', 4, '>i4')
    AMOUNT = ("amount", np.float64, 'd', 8, '>f8')  # 成交額
