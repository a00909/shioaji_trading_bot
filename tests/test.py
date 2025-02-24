from tools.app import App

app = App(init=True)
contract = app.api.Contracts.Futures.TMF.TMFR1
print(contract)