from shioaji.contracts import Contracts, Contract, StreamIndexContracts, StreamMultiContract
from tools.app import App


class ContractBrowser:

    def __init__(self, app: App):
        contracts: Contracts = app.api.Contracts
        self._levels: list[Contracts | StreamIndexContracts | StreamMultiContract | Contract] = [contracts]

    @property
    def _cur(self):
        return self._levels[-1]

    def _search_down(self, key: str):
        if isinstance(self._cur, Contract):
            return 'Is leaf now.'

        nxt = self._cur.get(key)
        if not nxt:
            return 'Key does not exist.'

        self._levels.append(nxt)
        return 'Ok'

    def _backspace(self):
        if isinstance(self._cur, Contracts):
            return 'Is root now.'

        self._levels.pop()
        return 'Ok'

    def _menu(self):
        if isinstance(self._cur, Contracts):
            return list(self._cur.__class__.model_fields.keys())

        if isinstance(self._cur, Contract):
            return repr(self._cur)

        return list(self._cur.keys())

    def start(self):
        print(
            'Contract browser v1.\n'
            'Type key to search down.\n'
            '"q" to last level.\n'
            '"e" to exit.'
        )
        while True:
            m = self._menu()
            print(m)
            op = input('Your input:')

            match op:
                case 'q':
                    print(self._backspace())
                case 'e':
                    print('Bye.')
                    break
                case _:
                    print(self._search_down(op))


if __name__ == "__main__":
    app = App(init=True)
    cb = ContractBrowser(app)
    cb.start()
    app.shut()
