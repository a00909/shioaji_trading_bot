from shioaji.contracts import Contracts, StreamIndexContracts, StreamMultiContract, Index, Stock, Future, Option

from tools.app import App

app = App(init=True)
contracts: Contracts = app.api.Contracts

keys = list(contracts.__class__.model_fields.keys())
print(keys)
l1 = contracts.get(keys[0])
l1_keys = list(l1.keys())
print(l1_keys)

l2: StreamMultiContract = l1.get(l1_keys[0])
l2_keys = list(l2.keys())
print(l2_keys)

l3: Index | Stock | Future | Option = l2.get(l2_keys[0])
print(l3)
app.shut()
