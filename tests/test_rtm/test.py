from tick_manager.rtm.realtime_tick_manager import RealtimeTickManager
from tools.app import App
import shioaji as sj
import pandas as pd

app = App(True)
rtm = RealtimeTickManager(
    app.api,
    app.redis,
    app.api.Contracts.Futures.TMF.TMFR1,
)


def print_df(rts: list[sj.TickFOPv1]):
    df = pd.DataFrame([vars(obj) for obj in rts])

    print(df)


def test_1():
    rtm.start()
    rtm.wait_for_ready()
    # print('in-day history finished. waiting for 5 seconds..')
    # time.sleep(5)

    # ticks = rtm.get_ticks_by_backtracking_time(timedelta(minutes=180))
    ticks = rtm.get_ticks_by_index(0)
    print_df(ticks)
    rtm.stop()
    app.shut()


def test_2():
    rtm.start()
    rtm.wait_for_ready()
    # print('in-day history finished. waiting for 5 seconds..')
    # time.sleep(5)

    # ticks = rtm.get_ticks_by_backtracking_time(timedelta(minutes=180))
    ticks = rtm.get_ticks_by_index(0)

    data = {
        'datetime': [pd.to_datetime(tick.datetime) for tick in ticks]  # 將 tick 的 datetime 提取並轉換為 datetime 格式
    }


    pd.set_option('display.max_rows', None)  # 無限制，顯示所有行
    df = pd.DataFrame(data)

    # 設定 datetime 為索引
    df.set_index('datetime', inplace=True)

    # 每 10 分鐘分組，計算 tick 數量
    tick_count = df.resample('10T').size()

    # 顯示結果
    print(tick_count)

    # 可視化
    # tick_count.plot(kind='bar', color='skyblue', title='Tick Count per 10 Minutes')
    # plt.ylabel('Tick Count')
    # plt.xlabel('Time Interval')
    # plt.xticks(rotation=45)
    # plt.tight_layout()
    # plt.show()

    rtm.stop()
    app.shut()
