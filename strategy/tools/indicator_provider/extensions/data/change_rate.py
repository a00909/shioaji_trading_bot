from typing_extensions import override

from strategy.tools.indicator_provider.extensions.data.indicator import Indicator


class ChangeRate(Indicator):
    def __init__(self):
        super().__init__()
        self.rsum = None
        self.tsum = None
        self.rtsum = None
        self.rsqsum = None

    @override
    def _calc(self):
        print(self.data_count, self.rsum, self.tsum, self.rtsum, self.rsqsum)
        if (self.data_count * self.rsqsum - self.rsum ** 2) == 0:
            return 0
        return (
                (self.data_count * self.rtsum - self.rsum * self.tsum) /
                (self.data_count * self.rsqsum - self.rsum ** 2)
        )
