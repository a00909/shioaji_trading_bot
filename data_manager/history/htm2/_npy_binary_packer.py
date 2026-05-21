import numpy as np

from data_manager.history.statics.np_ticks import NPTicks
from data_manager.history.statics.tick_field import TickField, FIELD_COUNT


def _get_dtype(symbol):
    """從 TickField 動態建構與 COPY BINARY 一致的 dtype"""
    fields = [
        ('nfields', '>i2'),  # 列頭：欄位數
    ]
    for f in TickField:
        fields.append((f'{f.prop_name}_len', '>i4'))
        fields.append((f.prop_name, f.np_fmt))
    fields.append(('sym_len', '>i4'))
    fields.append(('symbol', f'S{len(symbol)}'))
    return np.dtype(fields)


def npy_pack(symbol, np_ticks: NPTicks):
    n = len(np_ticks)
    dtype = _get_dtype(symbol)
    arr = np.empty(n, dtype=dtype)
    arr['nfields'] = FIELD_COUNT + 1
    for f in TickField:
        arr[f'{f.prop_name}_len'] = f.byte_len
        arr[f.prop_name] = np_ticks.__dict__[f.prop_name]

    arr['symbol'] = symbol.encode()
    arr['sym_len'] = len(symbol)
    return arr.tobytes()
