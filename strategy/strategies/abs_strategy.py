from abc import ABC, abstractmethod

from strategy.strategies.data import StrategySuggestion, EntryReport
from strategy.tools.indicator_provider import IndicatorProvider


class AbsStrategy(ABC):

    def __init__(self, ip: IndicatorProvider):
        self.ip = ip

    @abstractmethod
    def in_signal(self) -> StrategySuggestion | None:
        pass

    @abstractmethod
    def report_entry(self, er: EntryReport):
        pass

    @abstractmethod
    def out_signal(self) -> StrategySuggestion | None:
        pass

    @abstractmethod
    def name(self):
        pass
