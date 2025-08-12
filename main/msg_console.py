from tools.app import App

app = App(init=True)

print('===Interactive msg console===')

while True:
    op = input(
        '(u) check usage.\n'
        '(la) list account.\n'
    )
    match op:
        case 'u':
            print(app.api.usage(),'\n')
        case 'la':
            print(app.api.list_accounts(),'\n')
        case _:
            print('bye.')
            app.shut()
            break



