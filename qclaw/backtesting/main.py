import sys
from datetime import date

from qclaw.backtesting import graph_sth
from qclaw.backtesting.dc_backtesting_context import DonchianBacktestingContext

sys.path.insert(0, r'C:\Repos\shioaji_trading')

from qclaw.backtesting.backtesting_dc import BacktestingDC
from qclaw.backtesting.dc_stat import DonchianStat

start = date(2025, 11, 18)
end = date(2025, 11, 18)

i = input(
    "1. backtest dc\n"
    "2. stat dc\n"
    "3. graph dc\n"
    "0. exit\n"
)
match i:
    case '1':
        bt = BacktestingDC(start, end)
        bt.backtest()
        bt.results()
    case '2':
        test_thresholds = [5, 7, 10, 20, 50, 100, 150, 200]
        st = DonchianStat(start, end, test_thresholds)
        records = st.stat()
        from qclaw.backtesting.dc_stat_analyzer import analyze_dc_stat

        analyze_dc_stat(records, test_thresholds)
    case '3':
        dbc = DonchianBacktestingContext(start, end)
        graph_sth.graph(dbc)

