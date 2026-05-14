import sys
from datetime import date

sys.path.insert(0, r'C:\Repos\shioaji_trading')

from qclaw.backtesting.backtesting_dc import BacktestingDC
from qclaw.backtesting.dc_stat import DonchianStat

test_start = date(2025, 11, 18)
test_end = date(2026, 1, 17)

i = input(
    "1. backtest dc\n"
    "2. stat dc\n"
    "0. exit\n"
)
match i:
    case '1':
        bt = BacktestingDC(test_start, test_end)
        bt.backtest()
        bt.results()
    case '2':
        test_thresholds = [5, 7, 10, 20, 50, 100, 150, 200]
        st = DonchianStat(test_start, test_end, test_thresholds)
        records = st.stat()

        from qclaw.backtesting.dc_stat_analyzer import analyze_dc_stat

        analyze_dc_stat(records, test_thresholds)
