from __future__ import annotations

import sys

from grasp.branding import APP_DISPLAY_NAME, APP_ORGANIZATION
from grasp.qt_compat import QApplication
from grasp.ui.main_window import MainWindow


def main() -> int:
    app = QApplication(sys.argv)
    app.setApplicationName(APP_DISPLAY_NAME)
    app.setOrganizationName(APP_ORGANIZATION)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

