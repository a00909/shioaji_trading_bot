
from datetime import timedelta,datetime

now = datetime.now()
_now = now + timedelta(microseconds=1)

print(now == _now)
