from datetime import datetime


class BacktestingRecord:
    def __init__(self):
        self.direction = None
        self.entry_time = None
        self.entry_price = None
        self.exit_time = None
        self.exit_price = None
        self.peak_price = None

        self.tp_hit = None
        self.pnl = None
        self.day = None

    def to_dict(self):
        return {
            'direction': 'long' if self.direction == 1 else 'short',
            'entry_time': self.entry_time, 'entry_price': self.entry_price,
            'exit_time': self.exit_time, 'exit_price': self.exit_price,
            'peak_price': self.peak_price,
            'tp_hit': self.tp_hit, 'pnl': self.pnl,
            'day': self.entry_time.strftime('%Y-%m-%d'),
        }

    @classmethod
    def from_dict(cls, data):
        r = cls()
        r.direction = data['direction']
        r.entry_time = data['entry_time']
        r.entry_price = data['entry_price']
        r.exit_time = data['exit_time']
        r.exit_price = data['exit_price']
        r.peak_price = data['peak_price']
        r.tp_hit = data['tp_hit']
        r.pnl = data['pnl']
        r.day = datetime.strptime(data['day'],'%Y-%m-%d')
        return r
