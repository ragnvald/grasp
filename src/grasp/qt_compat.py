from __future__ import annotations

import os

QT_API = "PySide6"

from PySide6.QtCore import QObject, QRunnable, QSettings, QThreadPool, Qt, QTimer, QUrl, Signal, Slot
from PySide6.QtGui import QAction, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtWebChannel import QWebChannel

_WEBENGINE_DISABLED_VALUES = {"1", "true", "yes", "on"}
WEBENGINE_DISABLED_BY_ENV = os.environ.get("GRASP_DISABLE_WEBENGINE", "").strip().lower() in _WEBENGINE_DISABLED_VALUES

if WEBENGINE_DISABLED_BY_ENV:
    QWebEngineView = None
    QWebEnginePage = None
    QWebEngineSettings = None
else:
    try:
        from PySide6.QtWebEngineWidgets import QWebEngineView
    except Exception:
        QWebEngineView = None
    try:
        from PySide6.QtWebEngineCore import QWebEnginePage, QWebEngineSettings
    except Exception:
        QWebEnginePage = None
        QWebEngineSettings = None


WEBENGINE_AVAILABLE = QWebEngineView is not None
WEBENGINE_UNAVAILABLE_MESSAGE = (
    "Qt WebEngine is disabled for this run. Restart without --disable-webengine to try the embedded map again."
    if WEBENGINE_DISABLED_BY_ENV
    else "Qt WebEngine is not available in this environment."
)
