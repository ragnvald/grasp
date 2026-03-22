from __future__ import annotations

import sys

from grasp.branding import APP_DISPLAY_NAME, APP_ORGANIZATION
from grasp.runtime import configure_qt_runtime


sys.argv = [sys.argv[0], *configure_qt_runtime(sys.argv[1:])]

from grasp.qt_compat import QApplication, Qt
from grasp.ui.main_window import MainWindow


def main() -> int:
    if hasattr(QApplication, "setAttribute"):
        try:
            QApplication.setAttribute(Qt.AA_UseSoftwareOpenGL, True)
        except Exception:
            pass
    app = QApplication(sys.argv)
    app.setApplicationName(APP_DISPLAY_NAME)
    app.setOrganizationName(APP_ORGANIZATION)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
