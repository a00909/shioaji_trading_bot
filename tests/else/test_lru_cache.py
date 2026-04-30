import datetime
import time
from functools import lru_cache


@lru_cache
def count(n):
    c = 0
    while c < n:
        c += 1
        time.sleep(0.5)
    return c


start = datetime.datetime.now()
c = count(10)
print(c, datetime.datetime.now() - start)

start = datetime.datetime.now()
c = count(10)
print(c, datetime.datetime.now() - start)

count.cache_clear()

start = datetime.datetime.now()
c = count(10)
print(c, datetime.datetime.now() - start)