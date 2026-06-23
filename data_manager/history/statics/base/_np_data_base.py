from dataclasses import dataclass, is_dataclass, fields
from typing import Self, TypeVar

import numpy as np

from data_manager.history.statics.base._field_base import _FieldBase
from tools.time_utils import PG_EPOCH_OFFSET_S, PG_EPOCH_WITH_TZ_US


@dataclass
class _NpDataBase:
    ts: np.ndarray  # float64, Unix timestamps

    def __len__(self) -> int:
        return len(self.ts)

    def __post_init__(self):
        self._datetime64 = None
        self._ts_s = None

    def ts_seconds(self, reset=False):
        if reset or self._ts_s is None:
            self._ts_s = self.ts / 10 ** 6 + PG_EPOCH_OFFSET_S
        return self._ts_s

    def datetime64(self, reset=False):
        if reset or self._datetime64 is None:
            self._datetime64 = self.ts_seconds().astype('datetime64[s]') + np.timedelta64(8, 'h')
        return self._datetime64

    @classmethod
    def from_raw(cls, raw, field_cls: type[_FieldBase]):
        raw = {k.lower(): v for k, v in raw.__dict__.items()}

        d = {
            f: np.array(raw[f], dtype=t)
            for f, t in field_cls.dtype_map().items()
            if f != 'ts'
        }
        ts_arr = np.array(raw['ts'], dtype=np.float64)
        ts_arr //= 1000
        ts_arr -= PG_EPOCH_WITH_TZ_US
        d['ts'] = ts_arr
        return cls(**d)

    @classmethod
    def merge_slices(cls, slices: list[Self]) -> Self:
        """通用合併方法：自動將多個相同的 Data Class 實例內的 NumPy 陣列拼起來"""
        if not slices:
            raise ValueError("切片列表不能為空 (The slice list is empty.)")

        # 安全檢查：確保子類別確實是一個 dataclass
        if not is_dataclass(cls):
            raise TypeError(f"類別 {cls.__name__} 必須是 @dataclass 才能使用 merge_slices")

        # 核心魔法：透過 fields(cls) 動態獲取子類別的所有欄位名稱
        merged_fields = {
            field.name: np.concatenate([getattr(s, field.name) for s in slices])
            for field in fields(cls)
        }

        # 用解包 (Unpacking) 的方式實例化子類別並回傳
        return cls(**merged_fields)
