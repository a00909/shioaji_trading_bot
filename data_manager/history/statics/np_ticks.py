from dataclasses import dataclass

import numpy as np
from shioaji.base import BaseModel
from shioaji.data import Ticks

from data_manager.history.statics.tick_field import FIELD_TYPE_MAP
from tools.time_utils import PG_EPOCH_WITH_TZ_US


@dataclass
class NPTicks:
    """Single day's tick data as numpy arrays."""
    ts: np.ndarray  # float64, Unix timestamps
    close: np.ndarray  # float64
    volume: np.ndarray  # int64
    tick_type: np.ndarray  # int32
    bid_price: np.ndarray  # float64
    ask_price: np.ndarray  # float64
    bid_volume: np.ndarray  # int64
    ask_volume: np.ndarray  # int64

    def __len__(self) -> int:
        return len(self.ts)

    @classmethod
    def from_ticks(cls, ticks_raw: Ticks):
        """
        從raw sj Ticks轉為np array Ticks\n
        注意: ts 格式為 pg epoch
        :param ticks_raw:
        :return:
        """
        raw = ticks_raw.__dict__

        d = {
            f: np.array(raw[f], dtype=t)
            for f, t in FIELD_TYPE_MAP.items()
            if f != 'ts'
        }
        ts_arr = np.array(raw['ts'], dtype=np.float64)
        ts_arr //= 1000
        ts_arr -= PG_EPOCH_WITH_TZ_US
        d['ts'] = ts_arr
        return cls(**d)

    @staticmethod
    def merge_slices(slices: list['NPTicks']) -> 'NPTicks':
        if not slices:
            raise ValueError("The slice list is empty.")

        return NPTicks(  # 通常合併後日期可能跨天，這裡取末尾或自定義
            ts=np.concatenate([s.ts for s in slices]),
            close=np.concatenate([s.close for s in slices]),
            volume=np.concatenate([s.volume for s in slices]),
            tick_type=np.concatenate([s.tick_type for s in slices]),
            bid_price=np.concatenate([s.bid_price for s in slices]),
            ask_price=np.concatenate([s.ask_price for s in slices]),
            bid_volume=np.concatenate([s.bid_volume for s in slices]),
            ask_volume=np.concatenate([s.ask_volume for s in slices])
        )
