from datetime import timedelta, datetime
from typing import Callable

from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType
from strategy.tools.indicator_provider.indicator_provider import IndicatorProvider

__all__ = ["IndicatorFacade"]


class IndicatorFacade:
    def __init__(self, ip: IndicatorProvider):
        self._ip = ip

        self._unit = timedelta(minutes=1)
        self._len_long = timedelta(minutes=120)
        self._len_short = timedelta(minutes=15)
        self._len_covariance = timedelta(hours=2)
        self._len_covariance_short = timedelta(minutes=30)
        self._len_vma_short = timedelta(minutes=5)
        self._vma_long_times = 2
        self._bb_times = 1.5

        self.latest_price = IndicatorFacadeUnit(
            ip.latest_price,
            name='price'
        )

        self.pma_short = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_short),
            self._len_short,
            IndicatorType.PMA
        )

        self.pma_long = IndicatorFacadeUnit(
            lambda: ip.ma(self._len_long),
            self._len_long,
            IndicatorType.PMA
        )

        self.vma_long = IndicatorFacadeUnit(
            lambda: ip.vma(self._len_long, self._unit, self._vma_long_times),
            self._len_long,
            IndicatorType.VMA
        )

        self.vma_short = IndicatorFacadeUnit(
            lambda: ip.vma(self._len_short, self._unit),
            self._len_short,
            IndicatorType.VMA
        )

        self.sd = IndicatorFacadeUnit(
            lambda: ip.standard_deviation(self._len_long),
            self._len_long,
            IndicatorType.SD
        )

        self.bb_upper = IndicatorFacadeUnit(
            lambda: (self.pma_long() + self.sd() * self._bb_times),
            self._len_long,
            name=f'bb_upper_*{self._bb_times}'
        )

        self.bb_lower = IndicatorFacadeUnit(
            lambda: (self.pma_long() - self.sd() * self._bb_times),
            self._len_long,
            name=f'bb_lower_*{self._bb_times}'
        )

        self.covariance_long = IndicatorFacadeUnit(
            lambda: self._ip.covariance(self._len_covariance),
            self._len_covariance,
            IndicatorType.COVARIANCE
        )

        self.covariance_short = IndicatorFacadeUnit(
            lambda: self._ip.covariance(self._len_covariance_short),
            self._len_covariance_short,
            IndicatorType.COVARIANCE
        )

    def now(self):
        return self._ip.now


class IndicatorFacadeUnit:
    def __init__(self, getter, length=None, indicator_type=None, unit=None, name=None):
        self._indicator_type: IndicatorType = indicator_type
        self._getter: Callable = getter
        self._length: timedelta = length
        self._unit = unit
        self._name = name
        self._msg = None

    @property
    def name(self):
        if self._name:
            if self._length:
                return f'{self._name}_{self._length.total_seconds()}_s'
            else:
                return self._name
        else:
            return f'{self._indicator_type.value}_{self._length.total_seconds()}_s'

    @property
    def msg(self):
        return f'{self.name}: {self()}'

    def __call__(self, *args, **kwargs):
        return self._getter(*args, **kwargs)
