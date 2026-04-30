from typing_extensions import override

from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from tools.utils import decode_redis


class NetBuyRatio(Indicator):
    L2_SEPERATOR = '|'

    def __init__(self):
        super().__init__()
        self.active_buy_vol = None
        self.active_sell_vol = None

    @override
    def serialize(self, serial):
        data_str = super().serialize(serial) + self.L2_SEPERATOR

        data_str += (
            f'{self.active_buy_vol}{self.L2_SEPERATOR}'
            f'{self.active_sell_vol}'
        )

        return data_str

    @classmethod
    @override
    def deserialize(cls, data: str | bytes, from_subclass=False, subclass_instance=None):
        data = decode_redis(data)
        l2_data = data.split(cls.L2_SEPERATOR)
        instance = cls()
        super().deserialize(l2_data[0], from_subclass=True, subclass_instance=instance)

        values = l2_data[1].split(cls.L1_SEPERATOR)
        instance.active_buy_vol = values[0]
        instance.active_sell_vol = values[1]
        return instance

    @override
    def _calc(self):
        return (self.active_buy_vol - self.active_sell_vol) / (self.active_buy_vol + self.active_sell_vol)
