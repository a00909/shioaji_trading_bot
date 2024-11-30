import shioaji as sj
import pandas as pd


def tick_to_dict(tick: sj.TickFOPv1):
    return {
        'code': tick.code,
        'datetime': tick.datetime.isoformat(),  # 將datetime轉為ISO格式字串
        'open': str(tick.open),  # 將Decimal轉為字串以便於顯示
        'underlying_price': str(tick.underlying_price),
        'bid_side_total_vol': tick.bid_side_total_vol,
        'ask_side_total_vol': tick.ask_side_total_vol,
        'avg_price': str(tick.avg_price),
        'close': str(tick.close),
        'high': str(tick.high),
        'low': str(tick.low),
        'amount': str(tick.amount),
        'total_amount': str(tick.total_amount),
        'volume': tick.volume,
        'total_volume': tick.total_volume,
        'tick_type': tick.tick_type,
        'chg_type': tick.chg_type,
        'price_chg': str(tick.price_chg),
        'pct_chg': str(tick.pct_chg),
        'simtrade': tick.simtrade,
    }


def to_df(ticks: list[sj.TickFOPv1]):
    data = [tick_to_dict(tick) for tick in ticks]


    # 設定顯示所有行
    pd.set_option('display.max_rows', 10)

    # 設定顯示所有列而不自動換行
    pd.set_option('display.max_colwidth', None)

    # 設定顯示所有列
    pd.set_option('display.max_columns', None)

    df = pd.DataFrame(data)
    return df
