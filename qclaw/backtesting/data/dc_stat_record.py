from typing_extensions import override

from qclaw.backtesting.data.backtesting_record import BacktestingRecord


class DonchianStatRecord(BacktestingRecord):
    def __init__(self):
        super().__init__()
        self.max_accum = None
        self.threshold = None
        self.mae = 0  # 最大逆流幅度（點數），永遠為正或0

    @property
    def max_possible_pnl(self):
        return abs(self.peak_price - self.entry_price)

    @override
    def to_dict(self):
        d = super().to_dict()
        d['max_accum'] = self.max_accum
        d['max_possible_pnl'] = self.max_possible_pnl
        d['mae'] = self.mae
        d['threshold'] = self.threshold
        return d

    @override
    def from_dict(cls, data):
        c = super().from_dict(data)
        c.max_accum = data['max_accum']
        c.threshold = data.get('threshold')
        c.mae = data.get('mae', 0)
        return c
