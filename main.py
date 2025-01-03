from tools.main_app import MainApp
from ui.main_ui import MainUI

if __name__ == "__main__":
    app = MainApp()
    ui = MainUI(app)

    ui.start()
