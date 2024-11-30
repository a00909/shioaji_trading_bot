import time

import shioaji as sj
from shioaji.contracts import FetchStatus

print(sj.__version__)
# 1.0.0

api = sj.Shioaji(simulation=True) # 模擬模式
api.login(
    api_key="9zPeyquB5C6zAPXHhDDfMVJqizZi3HjKj1KtdzgkPMcu",
    secret_key="FXcmuoQRWnvShmak52T3BAEvRA49kPw7k1M8EkZssk1N"
)

while api.Contracts.status == FetchStatus.Fetching:
    print(f'Contracts status: {api.Contracts.status}')
    time.sleep(1)

if api.Contracts.status == FetchStatus.Fetched:
    api.quote.subscribe(
        api.Contracts.Futures.TMF.TMF202411,
        quote_type=sj.constant.QuoteType.Tick,
        version=sj.constant.QuoteVersion.v1,
    )

else:
    print(f'Failed to fetch data.')
