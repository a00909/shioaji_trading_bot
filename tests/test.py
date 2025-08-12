import pandas

from tick_manager.kbar_manager import KBarManager
from tools.app import App

app = App(init=True)
kbar_manager = KBarManager(app.api, None)
kbars = kbar_manager._get_from_api(
    contract=app.api.Contracts.Futures.TMF.TMFR1,
    start='2025-03-19',
    end='2025-03-20'
)
df = pandas.DataFrame({**kbars})
df.ts = pandas.to_datetime(df.ts)
print(df)