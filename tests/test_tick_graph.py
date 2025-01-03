from data.tick_fop_v1d1 import TickFOPv1D1
from strategy.tools.indicator_provider.extensions.data.indicator import Indicator
from tools import RedisManager
from tools.plotter import plotter

keys = [
    "indicator:TXFR1:IndicatorType.VMA30.0s:2025.01.01",
    "indicator:TXFR1:IndicatorType.PMA300.0s:2025.01.01",
    "indicator:TXFR1:IndicatorType.VMA3600.0s:2025.01.01",
    "indicator_serial:TXFR1:IndicatorType.PMA3600.0s:2025.01.01",
    "indicator:TXFR1:IndicatorType.PMA3600.0s:2025.01.01",
    "indicator_serial:TXFR1:IndicatorType.VMA3600.0s:2025.01.01",
    "indicator_serial:TXFR1:IndicatorType.VMA30.0s:2025.01.01",
    "realtime.tick:TXFR1:2025.01.01",
    "indicator_serial:TXFR1:IndicatorType.PMA300.0s:2025.01.01",
]
redis = RedisManager().redis
ticks_raw = redis.zrange("realtime.tick:TXFR1:2025.01.03", 0, -1)
pma_3600s_raw = redis.zrange("indicator:TXFR1:IndicatorType.PMA3600.0s:2025.01.03", 0, -1)
pma_300s_raw = redis.zrange("indicator:TXFR1:IndicatorType.PMA300.0s:2025.01.03", 0, -1)

ticks = [TickFOPv1D1.deserialize(d) for d in ticks_raw]
pma_3600s = [Indicator.deserialize(d) for d in pma_3600s_raw]
pma_300s = [Indicator.deserialize(d) for d in pma_300s_raw]

plotter.active()

for t in ticks:
    plotter.add_points('price', (t.datetime, t.close))

for p in pma_3600s:
    plotter.add_points('pma3600s', (p.datetime, p.value))

for p in pma_300s:
    plotter.add_points('pma300s', (p.datetime, p.value))

plotter.plot()
