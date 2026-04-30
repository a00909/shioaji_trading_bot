import sys
sys.path.insert(0, r'C:\Repos\shioaji_trading')

from qclaw.backtesting.backtesting_dc import BacktestingDC
from qclaw.backtesting.dc_stat import DonchianStat

TEST_DATES = [
    '2025-11-18',
    '2025-11-19', '2025-11-20', '2025-11-21', '2025-11-24',
    '2025-11-25', '2025-11-26', '2025-11-27', '2025-11-28', '2025-12-01',
    '2025-12-02', '2025-12-03', '2025-12-04', '2025-12-05', '2025-12-08',
    '2025-12-09', '2025-12-10', '2025-12-11', '2025-12-12', '2025-12-15',
    '2025-12-16', '2025-12-17', '2025-12-18', '2025-12-19', '2025-12-22',
    '2025-12-23', '2025-12-24', '2025-12-25', '2025-12-26', '2025-12-29',
    '2025-12-30', '2025-12-31', '2026-01-01', '2026-01-02', '2026-01-05',
    '2026-01-06', '2026-01-07', '2026-01-08', '2026-01-09', '2026-01-12',
    '2026-01-13', '2026-01-14', '2026-01-15', '2026-01-16', '2026-01-19',
    '2026-01-20', '2026-01-21', '2026-01-22', '2026-01-23', '2026-01-26',
    '2026-01-27', '2026-01-28', '2026-01-29', '2026-01-30', '2026-02-02',
    '2026-02-03', '2026-02-04', '2026-02-05', '2026-02-06', '2026-02-09',
]

i = input(
    "1. backtest dc\n"
    "2. stat dc\n"
    "0. exit\n"
)
match i:
    case '1':
        bt = BacktestingDC(TEST_DATES)
        bt.backtest()
        bt.results()
    case '2':
        test_thresholds = [5, 7, 10, 20, 50, 100,150,200]
        st = DonchianStat(TEST_DATES,test_thresholds)
        records = st.stat()

        from qclaw.backtesting.dc_stat_analyzer import analyze_dc_stat
        analyze_dc_stat(records, test_thresholds)



