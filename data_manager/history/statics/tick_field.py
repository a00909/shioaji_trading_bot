import struct
from enum import Enum, StrEnum

import numpy as np

__all__ = [
    'TickField',
    'FIELD_COUNT',
    'TICKS_FIELDS',
    'FIELD_TYPE_MAP',
    'FIELD_LENS_LIST',
    'PACK_FMT_TEMPLATE',
    'UNPACK_STRUCT'
]


class TickField(StrEnum):
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

    def __new__(cls, prop_name: str, *args):
        # 1. 攔截 Tuple，拿第一個元素建立真正的字串本體，消滅 Signature 衝突
        obj = str.__new__(cls, prop_name)
        obj._value_ = prop_name
        return obj



    def __init__(self, prop_name: str, np_type: type, struct_fmt: str, byte_len: int, np_fmt: str):
        # 2. 把其餘硬核元資料綁在成員身上
        self.prop_name = prop_name
        self.np_type = np_type
        self.struct_fmt = struct_fmt
        self.byte_len = byte_len
        self.np_fmt = np_fmt


# ----------------------------------------------------
# 🚀 衍生常數自動拼裝（以下全部自動對齊，100% 不會出錯）
# ----------------------------------------------------
FIELD_COUNT = len(TickField)
TICKS_FIELDS = tuple(f.prop_name for f in TickField)
FIELD_TYPE_MAP = {f.prop_name: f.np_type for f in TickField}
FIELD_LENS_LIST = tuple(f.byte_len for f in TickField)
PACK_FMT_TEMPLATE = "!hI" + "I".join(f.struct_fmt for f in TickField) + "I{0}s" # deprecated
UNPACK_STRUCT = struct.Struct('>2x4x' + '4x'.join(f.struct_fmt for f in TickField))
