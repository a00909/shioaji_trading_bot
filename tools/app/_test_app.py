from tools.app.app import App

app = App()

sj = app.api
contract = sj.Contracts.Futures.TMF.TMFR1
print(contract)
