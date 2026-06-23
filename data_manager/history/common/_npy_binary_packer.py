import numpy as np

from data_manager.history.statics.base._field_base import _FieldBase
from data_manager.history.statics.base._np_data_base import _NpDataBase


def _pack_dtype(symbol, field_enum: type[_FieldBase]):
    """從 TickField 動態建構與 COPY BINARY 一致的 dtype"""
    fields = [
        ('nfields', '>i2'),  # 列頭：欄位數
    ]
    for f in field_enum:
        fields.append((f'{f.prop_name}_len', '>i4'))
        fields.append((f.prop_name, f.np_fmt))
    fields.append(('sym_len', '>i4'))
    fields.append(('symbol', f'S{len(symbol)}'))
    return np.dtype(fields)


def npy_pack(symbol, np_data: _NpDataBase, field_enum: type[_FieldBase]):
    n = len(np_data)
    dtype = _pack_dtype(symbol, field_enum)
    arr = np.empty(n, dtype=dtype)
    arr['nfields'] = field_enum.count() + 1
    for f in field_enum:
        arr[f'{f.prop_name}_len'] = f.byte_len
        arr[f.prop_name] = np_data.__dict__[f.prop_name]

    arr['symbol'] = symbol.encode()
    arr['sym_len'] = len(symbol)
    return arr.tobytes()


def _unpack_dtype(field_enum: type[_FieldBase]):
    names = []
    formats = []
    offsets = []

    pos = 2
    for f in field_enum:
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


def npy_unpack[D:_NpDataBase](field_enum: type[_FieldBase], np_data_type: type[D], raw: bytes) -> D:
    dtype = _unpack_dtype(field_enum)
    arr = np.frombuffer(raw, dtype=dtype)
    return np_data_type(**{name: arr[name] for name in arr.dtype.names})
