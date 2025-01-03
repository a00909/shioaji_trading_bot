from typing import Type

from strategy.tools.indicator_provider.extensions.data.indicator_type import IndicatorType

indicator_type = IndicatorType.from_string('pma')
print(type(indicator_type),indicator_type)
print(type(IndicatorType.PMA),indicator_type.PMA)

print(type(indicator_type) == IndicatorType)
print(type(indicator_type) == Type[IndicatorType])