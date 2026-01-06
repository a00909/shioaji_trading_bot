from shioaji.contracts import Contracts, Contract, StreamIndexContracts, StreamMultiContract
from tools.app import App


class ContractBrowser:

    def __init__(self, app: App):
        self._cur_level = 0
        self._levels: list[Contracts | StreamIndexContracts | StreamMultiContract | Contract] = []
        # self._keys = []

        contracts: Contracts = app.api.Contracts
        self._levels.append(contracts)
        # self._keys.append(list(contracts.__class__.model_fields.keys()))

    @property
    def _cur(self):
        return self._levels[self._cur_level]

    def _search_down(self, key: str):
        if isinstance(self._cur, Contract):
            return 'Is leaf now.'

        nxt = self._cur.get(key)
        if not nxt:
            return 'Key does not exist.'

        self._levels.append(nxt)
        self._cur_level += 1
        return 'Ok'

    def _backspace(self):
        if isinstance(self._cur, Contracts):
            return 'Is root now.'

        self._levels.pop()
        self._cur_level -= 1
        return 'Ok'

    def _menu(self):
        if isinstance(self._cur, Contracts):
            return self._format_menu(list(self._cur.__class__.model_fields.keys()))

        if isinstance(self._cur, Contract):
            return self._cur

        return self._format_menu(list(self._cur.keys()))

    @staticmethod
    def _format_menu(menu: list[str]) -> str:
        if not menu:
            return ""

        lines = [
            ",".join(menu[i:i + 10])
            for i in range(0, len(menu), 10)
        ]
        return "\n".join(lines)

    def start(self):
        print(
            'Contract browser v1.\n'
            'Type key to search down.\n'
            '"q" to last level.\n'
            '"e" to exit.'
        )
        while True:
            m = self._menu()
            if isinstance(m, list):
                print(
                    f'Selections: {m}'
                )
            else:
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
