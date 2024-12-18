from datetime import datetime, timedelta

from tick_manager.rtm_extensions.backtracking_time_getter import BacktrackingTimeGetter
from tools.app import App
from tools.constants import DEFAULT_TIMEZONE
from tools.utils import default_tickfopv1


def get_key():
    return 'test:bttg'


def print_nl_by_5(data):
    for e, d in enumerate(data):
        print(d, end=' ')
        if (e + 1) % 5 == 0:
            print()
    print()


app = App(init=True, init_api=False)

bttg = BacktrackingTimeGetter(app.redis, get_key)

start = datetime.strptime('2024-12-12', '%Y-%m-%d').replace(tzinfo=DEFAULT_TIMEZONE)

data_exists = app.redis.exists(get_key())
if not data_exists:
    data = {}
    for i in range(100):
        tick = default_tickfopv1()
        tick.datetime = start + timedelta(seconds=1 * i)
        s = tick.serialize(i)
        data[s] = tick.datetime.timestamp()

    app.redis.zadd(get_key(), data)

# test first get
fg = bttg.get(
    start + timedelta(seconds=45),
    start + timedelta(seconds=55)
)
print('result first get:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in fg])
print('buffer:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in bttg.buffer])
print('start:', bttg.last_start.time(), 'end', bttg.last_end.time())
print()

# test r range
fg = bttg.get(
    start + timedelta(seconds=50),
    start + timedelta(seconds=65)
)
print('result r range:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in fg])
print('buffer:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in bttg.buffer])
print('start:', bttg.last_start.time(), 'end', bttg.last_end.time())
print()
# test l range
fg = bttg.get(
    start + timedelta(seconds=35),
    start + timedelta(seconds=50)
)
print('result l range:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in fg])
print('buffer:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in bttg.buffer])
print('start:', bttg.last_start.time(), 'end', bttg.last_end.time())
print()
# test lr range
fg = bttg.get(
    start + timedelta(seconds=25),
    start + timedelta(seconds=75)
)
print('result lr:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in fg])
print('buffer:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in bttg.buffer])
print('start:', bttg.last_start.time(), 'end', bttg.last_end.time())
print()
# test inner range
fg = bttg.get(
    start + timedelta(seconds=40),
    start + timedelta(seconds=60)
)
print('result inner:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in fg])
print('buffer:')
print_nl_by_5([f'{f.datetime.minute}:{f.datetime.second}' for f in bttg.buffer])
print('start:', bttg.last_start.time(), 'end', bttg.last_end.time())
