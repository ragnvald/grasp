from __future__ import annotations

import os
import sys

from grasp.branding import APP_DISPLAY_NAME, APP_ORGANIZATION


def _append_env_flag(name: str, flag: str) -> None:
    current = os.environ.get(name, "").strip()
    if not current:
        os.environ[name] = flag
        return
    flags = current.split()
    if flag not in flags:
        os.environ[name] = f"{current} {flag}".strip()


def configure_qt_runtime() -> None:
    if sys.platform.startswith("win"):
        # Favor software rendering on Windows to avoid Qt WebEngine GPU/context crashes on mixed drivers/VM setups.
        os.environ.setdefault("QT_OPENGL", "software")
        os.environ.setdefault("QT_QUICK_BACKEND", "software")
        os.environ.setdefault("QSG_RHI_PREFER_SOFTWARE_RENDERER", "1")
        _append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu")
        _append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu-compositing")
        _append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-features=VizDisplayCompositor")


configure_qt_runtime()

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
