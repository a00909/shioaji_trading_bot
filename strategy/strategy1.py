import datetime
import threading
import time
import pandas as pd
import numpy as np
from shioaji.order import Trade
from sklearn.linear_model import LinearRegression

from strategy.strategy_base import StrategyBase


class Strategy1(StrategyBase):

    def __init__(self, rtm, htm, op):
        super().__init__(rtm, htm, op)
        self.run = False
        self.thread = threading.Thread(target=self.strategy_loop)
        self.trades = None

    def run_strategy(self):
        self.run = True
        self.thread.start()

    def stop_strategy(self, close_all=True):
        self.run = False
        self.thread.join()
        if close_all:
            self.realtime_tick_manager.api.update_status(cb=self.chk_trade_cb)

    def ma(self, length: datetime.timedelta):
        ticks = self.realtime_tick_manager.get_ticks_by_backtracking_time(length)
        avg = sum([tick.close for tick in ticks]) / len(ticks)
        return avg

    def is_increasing(self, length: datetime.timedelta, window_size=3):
        ticks = self.realtime_tick_manager.get_ticks_by_backtracking_time(length)
        series = pd.Series([tick.close for tick in ticks])

        # 計算簡單移動平均
        sma = series.rolling(window=window_size).mean()

        # 準備數據進行線性回歸分析
        x = np.arange(len(series)).reshape(-1, 1)  # 特徵：索引
        y = series.values.reshape(-1, 1)  # 標籤：數值

        # 創建線性回歸模型並擬合數據
        model = LinearRegression()
        model.fit(x, y)

        # 獲取斜率
        slope = model.coef_[0][0]

        # 判斷趨勢
        if slope > 0:
            trend_status = "上升趨勢"
        else:
            trend_status = "無上升趨勢"

        return slope > 0, slope

    def order_cb(self, trade: Trade):
        print(f'order callback:\n{trade}')

    def chk_trade_cb(self, trades: list[Trade]):
        print(f'chk_trade callback:\n{trades}')
        self.trades = trades
        total_qty = 0
        for t in trades:
            total_qty += t.order.quantity

        print(f'total quantity: {total_qty}, will be close.')
        self.order_placer.simple_sell(total_qty, self.order_cb)

    def strategy_loop(self):
        ma_len_minute = 0.5
        start_time = datetime.datetime.now()
        position_hold = False
        chk_timedelta = datetime.timedelta(minutes=ma_len_minute)

        while True:
            waiting_time = chk_timedelta - (datetime.datetime.now() - start_time)
            if waiting_time > datetime.timedelta(seconds=0):
                print(f'waiting for data...({waiting_time} minute left)')
                time.sleep(5)
                continue

            self.realtime_tick_manager.tick_received_event.wait()
            self.realtime_tick_manager.tick_received_event.clear()

            latest_tick = self.realtime_tick_manager.get_ticks_by_backward_idx(0)[0]
            ma = self.ma(chk_timedelta)
            is_increasing, slope = self.is_increasing(chk_timedelta)

            print(f'newest price: {latest_tick.close}, 30s_ma: {ma}, slope: {slope}, is_increasing: {is_increasing}')

            if latest_tick.close < ma and is_increasing:
                print(f'signal confirmed.')

                if not position_hold:
                    position_hold = True
                    print('position bought 1.')
                    self.order_placer.simple_buy(cb=self.order_cb)

            if latest_tick.close > ma and slope < 0 and position_hold:
                position_hold = False
                self.order_placer.close_all()

            time.sleep(0.5)
