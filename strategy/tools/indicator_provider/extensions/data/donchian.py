from typing_extensions import override

from strategy.tools.indicator_provider.extensions.data.indicator import Indicator


class Donchian(Indicator):
    def __init__(self):
        super().__init__()
        self.h = None
        self.l = None

        self.h_breakthrough = False
        self.l_breakthrough = False

        self.hh_accumulation = 0
        self.ll_accumulation = 0

        self.hl_accumulation = 0
        self.lh_accumulation = 0

        self.idle_accumulation = 0


        self.pivot_price = 0
        self.pivot_price_changed = False
        self.pivot_price_serial = 0

    def _calc(self):
        return tuple([self.h, self.l, self.h_prev, self.l_prev])
