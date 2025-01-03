import threading

from tools.main_app import MainApp

app = MainApp()
app.start()


def loop():
    while True:
        msg = (
            f'enter "e" to exit.'
        )
        i = input(msg)

        match i:
            case 'e':
                app.stop()
                print('bye bye.')
                break
            case _:
                pass


threading.Thread(target=loop).start()
