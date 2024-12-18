from datetime import datetime

from shioaji.constant import Action


class StrategySuggestion:
    def __init__(self, action=None, quantity=None, valid=None):
        self.action: Action = action
        self.quantity = quantity
        self.valid = valid


class EntryReport(StrategySuggestion):
    def __init__(self, ss: StrategySuggestion = None):
        self.deal_time: datetime = None
        self.deal_price: float = None

        super().__init__()
        if ss:
            self.action = ss.action
            self.quantity = ss.quantity
            self.valid = ss.valid
