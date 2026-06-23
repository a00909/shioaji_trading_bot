import struct
from enum import StrEnum
from functools import cache
from typing import ClassVar, Self

import numpy as np


class _FieldBase(StrEnum):
    TS: ClassVar[Self]

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

    @classmethod
    @cache
    def count(cls):
        return len(cls)

    @classmethod
    @cache
    def names(cls) -> tuple[str, ...]:
        return tuple(f.prop_name for f in cls)

    @classmethod
    @cache
    def dtype_map(cls) -> dict:
        return {f.prop_name: f.np_type for f in cls}

    @classmethod
    @cache
    def lens(cls) -> tuple[int, ...]:
        return tuple(f.byte_len for f in cls)

    @classmethod
    @cache
    def unpack_struct(cls) -> struct.Struct:
        return struct.Struct('>2x4x' + '4x'.join(f.struct_fmt for f in cls))
