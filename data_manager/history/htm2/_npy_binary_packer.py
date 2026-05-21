import numpy as np

from data_manager.history.statics.np_ticks import NPTicks
from data_manager.history.statics.tick_field import TickField, FIELD_COUNT


def _pack_dtype(symbol):
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
    dtype = _pack_dtype(symbol)
    arr = np.empty(n, dtype=dtype)
    arr['nfields'] = FIELD_COUNT + 1
    for f in TickField:
        arr[f'{f.prop_name}_len'] = f.byte_len
        arr[f.prop_name] = np_ticks.__dict__[f.prop_name]

    arr['symbol'] = symbol.encode()
    arr['sym_len'] = len(symbol)
    return arr.tobytes()


def _unpack_dtype(symbol):
    names = []
    formats = []
    offsets = []

    pos = 2
    for f in TickField:
        pos += 4
        names.append(f.prop_name)
        formats.append(f.np_fmt)
        offsets.append(pos)
        pos += f.byte_len

    return np.dtype({
        'names': names,
        'formats': formats,
        'offsets': offsets,
        'itemsize': pos,
    })


def npy_unpack(symbol, raw: bytes):
    dtype = _unpack_dtype(symbol)
    arr = np.frombuffer(raw, dtype=dtype)
    return NPTicks(**{name: arr[name] for name in arr.dtype.names})
