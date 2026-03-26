from __future__ import annotations

from dataclasses import replace
from datetime import datetime
import math
from pathlib import Path
import shutil
import traceback
from time import monotonic, sleep

from grasp.branding import (
    APP_ACRONYM,
    APP_AUTHOR,
    APP_DISPLAY_NAME,
    APP_LINKEDIN_URL,
    APP_MISSION,
    APP_REPOSITORY_URL,
    APP_TAGLINE,
    APP_WINDOW_TITLE,
    DEFAULT_EXPORT_GPKG_NAME,
)
from grasp.catalog.repository import CatalogRepository
from grasp.data_languages import (
    MANAGED_DATA_LANGUAGE_NOT_SET_LABEL,
    MANAGED_DATA_LANGUAGE_OPTIONS,
    display_managed_data_language,
    normalize_managed_data_language,
)
from grasp.export.service import ExportService
from grasp.ingest.service import IngestService, MAX_AUTO_VISIBLE_DATASETS, MAX_AUTO_VISIBLE_FEATURES
from grasp.intelligence.providers import (
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_TIMEOUT_S,
    DuckDuckGoSearchProvider,
    HeuristicClassificationProvider,
    OpenAIClassificationProvider,
)
from grasp.intelligence.service import IntelligenceService, SearchService
from grasp.models import DatasetRecord, DatasetUnderstanding
from grasp.name_simplification import suggest_simplified_dataset_name
from grasp.qt_compat import (
    QAction,
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QColor,
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
    QPainter,
    QPlainTextEdit,
    QPixmap,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QSplitter,
    QSplitterHandle,
    QStatusBar,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QTimer,
    QThreadPool,
    QTreeWidget,
    QTreeWidgetItem,
    QUrl,
    QVBoxLayout,
    QWidget,
    Qt,
    QWebChannel,
    QWebEnginePage,
    QWebEngineSettings,
    QWebEngineView,
    Signal,
    WEBENGINE_AVAILABLE,
    WEBENGINE_UNAVAILABLE_MESSAGE,
)
from grasp.source_style import describe_source_style_evidence
from grasp.ui.map_bridge import MapBridge
from grasp.ui.settings_dialog import MODEL_OPTIONS
from grasp.ui.workers import FunctionWorker
from grasp.settings import AppSettings, SettingsStore
from grasp.styling import StyleService
from grasp.workspace import catalog_exists, display_group_name, ensure_workspace, sanitize_group_id


REGROUP_OTHERS_GROUP_NAME = "Others"
REGROUP_HINT_PREPARATION_TIMEOUT_S = 240.0
REGROUP_TOTAL_TIMEOUT_S = 240.0
REGROUP_GROUP_COUNT_TOLERANCE_RATIO = 0.10
INITIAL_HEURISTIC_CLASSIFICATION_TIMEOUT_S = 60.0
REVIEW_JOB_STALE_LOCK_TIMEOUT_S = 300.0
REMOTE_AI_REQUEST_COOLDOWN_S = 0.35
UNDERSTANDING_PERSIST_BATCH_SIZE = 24
MAP_HTTP_USER_AGENT = f"GRASP-Desktop (+{APP_REPOSITORY_URL})"
UI_ASSETS_DIR = Path(__file__).resolve().parent / "assets"
ABOUT_ILLUSTRATION_PATH = UI_ASSETS_DIR / "about_vacuum_gpkg.svg"
APP_ICON_PATH = UI_ASSETS_DIR / "grasp_app_icon.svg"
# These labels reflect the current archive-oriented wording requested for the import flow.
LOAD_ARCHIVE_LABEL = "Load data from folder"
REBUILD_ARCHIVE_LABEL = "Rebuild archive"
TAB_PAGE_MARGIN_PX = 6
MANAGE_ACTION_BUTTON_WIDTH_PX = 216
MANAGE_ACTION_BUTTON_HEIGHT_PX = 32


class DatasetTreeWidget(QTreeWidget):
    orderingChanged = Signal()

    def dropEvent(self, event) -> None:  # type: ignore[override]
        super().dropEvent(event)
        self.orderingChanged.emit()


class SortableTableWidgetItem(QTableWidgetItem):
    def __init__(self, text: str, sort_value=None) -> None:
        super().__init__(text)
        self._sort_value = text if sort_value is None else sort_value

    def __lt__(self, other) -> bool:  # type: ignore[override]
        if isinstance(other, SortableTableWidgetItem):
            return self._sort_value < other._sort_value
        return super().__lt__(other)


class CanvasSplitterHandle(QSplitterHandle):
    _background_color = QColor("#f3ecdf")
    _shadow_color = QColor("#d0bc97")
    _highlight_color = QColor("#fffaf0")

    def paintEvent(self, event) -> None:  # type: ignore[override]
        _ = event
        painter = QPainter(self)
        painter.fillRect(self.rect(), self._background_color)

        if self.orientation() == Qt.Horizontal:
            line_extent = max(36, min(72, self.height() // 6))
            top = max(0, (self.height() - line_extent) // 2)
            center_x = self.width() // 2
            painter.fillRect(center_x - 1, top, 1, line_extent, self._shadow_color)
            painter.fillRect(center_x + 1, top, 1, line_extent, self._highlight_color)
            return

        line_extent = max(36, min(72, self.width() // 6))
        left = max(0, (self.width() - line_extent) // 2)
        center_y = self.height() // 2
        painter.fillRect(left, center_y - 1, line_extent, 1, self._shadow_color)
        painter.fillRect(left, center_y + 1, line_extent, 1, self._highlight_color)


class CanvasSplitter(QSplitter):
    def __init__(self, orientation, parent=None) -> None:
        super().__init__(orientation, parent)
        self.setHandleWidth(12)

    def createHandle(self) -> QSplitterHandle:  # type: ignore[override]
        return CanvasSplitterHandle(self.orientation(), self)


if QWebEnginePage is not None:
    class LoggingWebEnginePage(QWebEnginePage):
        def __init__(self, log_callback, parent=None) -> None:
            super().__init__(parent)
            self._log_callback = log_callback

        def javaScriptConsoleMessage(self, level, message: str, line_number: int, source_id: str) -> None:  # type: ignore[override]
            level_name = getattr(level, "name", str(level))
            text = f"JS console [{level_name}] {source_id}:{line_number} - {message}"
            try:
                self._log_callback(text)
            except Exception:
                pass
            try:
                super().javaScriptConsoleMessage(level, message, line_number, source_id)
            except Exception:
                pass
else:
    LoggingWebEnginePage = None


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_WINDOW_TITLE)
        self.resize(1460, 920)

        self.thread_pool = QThreadPool.globalInstance()
        self.current_workspace = None
        self.repository: CatalogRepository | None = None
        self.ingest_service: IngestService | None = None
        self.settings_store = SettingsStore()
        self.current_settings = self.settings_store.load()
        self.intelligence_service = None
        self.heuristic_intelligence_service = IntelligenceService(classifier=HeuristicClassificationProvider())
        self.search_service = None
        self.style_service = StyleService()
        self.export_service: ExportService | None = None
        self.map_bridge: MapBridge | None = None
        self.map_channel = None
        self._populating_tree = False
        self._populating_inspector = False
        self._checked_dataset_id_set: set[str] = set()
        self._active_workers: dict[int, FunctionWorker] = {}
        self._map_initialized = False
        self._map_page_ready = False
        self._map_refresh_pending = False
        self._review_job_running = False
        self._syncing_dataset_selection = False
        self._dataset_browser_row_ids: list[str] = []
        self._background_progress_token = 0
        self._active_background_progress_token = 0
        self._background_activity_names: dict[int, str] = {}
        self._background_activity_last_status: dict[int, str] = {}
        self._background_activity_progress_value: int | None = None
        self._background_activity_started_at: float | None = None
        self._background_activity_worker_signal_at: float | None = None
        self._updating_review_dataset_splitter = False
        self._info_sources_splitter_initialized = False
        self._background_heartbeat_timer = QTimer(self)
        self._background_heartbeat_timer.setInterval(15000)
        self._background_heartbeat_timer.timeout.connect(self._emit_background_activity_heartbeat)

        self._rebuild_ai_services()
        self._build_ui()
        self._build_menu()

    def _build_ui(self) -> None:
        central_host = QWidget()
        central_host.setObjectName("CentralHost")
        central_layout = QVBoxLayout(central_host)
        central_layout.setContentsMargins(0, 0, 0, 0)
        central_layout.setSpacing(6)

        self._build_log_window()

        self.global_actions_host = QWidget()
        self.global_actions_host.setObjectName("GlobalActionsBar")
        global_actions_layout = QHBoxLayout(self.global_actions_host)
        global_actions_layout.setContentsMargins(10, 8, 10, 0)
        global_actions_layout.setSpacing(6)
        global_actions_layout.addStretch(1)

        self.log_button = QPushButton("Logs")
        self.log_button.setObjectName("CornerLogButton")
        self.log_button.setMinimumWidth(82)
        self.log_button.setMaximumWidth(104)
        self.log_button.setMaximumHeight(24)
        self.log_button.setToolTip("Open the global activity log.")
        self.log_button.clicked.connect(self.open_log_window)
        global_actions_layout.addWidget(self.log_button)

        self.exit_button = QPushButton("Exit")
        self.exit_button.setObjectName("CornerExitButton")
        self.exit_button.setMinimumWidth(58)
        self.exit_button.setMaximumWidth(72)
        self.exit_button.setMaximumHeight(24)
        self.exit_button.setToolTip(f"Close {APP_DISPLAY_NAME}.")
        self.exit_button.clicked.connect(self.close)
        global_actions_layout.addWidget(self.exit_button)

        central_layout.addWidget(self.global_actions_host, 0)

        self.tabs = QTabWidget()
        self.tabs.setObjectName("MainTabs")
        self.tabs.setUsesScrollButtons(True)
        self.tabs.setElideMode(Qt.ElideRight)
        self.tabs.tabBar().setExpanding(False)
        self.tabs.currentChanged.connect(self.on_tab_changed)
        central_layout.addWidget(self.tabs, 1)
        self.setCentralWidget(central_host)

        self.import_tab = QWidget()
        self.import_tab.setObjectName("ImportTab")
        self.review_datasets_tab = QWidget()
        self.review_datasets_tab.setObjectName("ReviewDatasetsTab")
        self.info_sources_tab = QWidget()
        self.info_sources_tab.setObjectName("InfoSourcesTab")
        self.datasets_overview_tab = QWidget()
        self.datasets_overview_tab.setObjectName("DatasetsOverviewTab")
        self.map_tab = QWidget()
        self.map_tab.setObjectName("MapTab")
        self.settings_tab = QWidget()
        self.settings_tab.setObjectName("SettingsTab")
        self.about_tab = QWidget()
        self.about_tab.setObjectName("AboutTab")

        self.tabs.addTab(self.import_tab, "Import")
        self.tabs.addTab(self.datasets_overview_tab, "Datasets overview")
        self.tabs.addTab(self.review_datasets_tab, "Review datasets")
        self.tabs.addTab(self.info_sources_tab, "Manage data")
        self.tabs.addTab(self.map_tab, "Map")
        self.tabs.addTab(self.settings_tab, "Settings")
        self.tabs.addTab(self.about_tab, "About")

        self._build_import_tab()
        self._build_review_datasets_tab()
        self._build_info_sources_tab()
        self._build_datasets_overview_tab()
        self._build_map_tab()
        self._build_settings_tab()
        self._build_about_tab()
        self._apply_canvas_theme()

        self.setStatusBar(QStatusBar())
        self.statusBar().showMessage("Select a folder to create or reopen a local GRASP catalog.")

    def _build_menu(self) -> None:
        file_menu = self.menuBar().addMenu("&File")
        browse_action = QAction("Browse Folder", self)
        browse_action.triggered.connect(self.browse_folder)
        file_menu.addAction(browse_action)

        self.load_existing_action = QAction(LOAD_ARCHIVE_LABEL, self)
        self.load_existing_action.triggered.connect(self.load_existing_catalog)
        file_menu.addAction(self.load_existing_action)

        scan_action = QAction(REBUILD_ARCHIVE_LABEL, self)
        scan_action.triggered.connect(self.start_scan)
        file_menu.addAction(scan_action)

        self.reset_data_action = QAction("Reset All Data", self)
        self.reset_data_action.triggered.connect(self.reset_all_data)
        file_menu.addAction(self.reset_data_action)

        settings_action = QAction("Settings", self)
        settings_action.triggered.connect(lambda: self.tabs.setCurrentWidget(self.settings_tab))
        file_menu.addAction(settings_action)

        about_action = QAction("About", self)
        about_action.triggered.connect(lambda: self.tabs.setCurrentWidget(self.about_tab))
        file_menu.addAction(about_action)

        file_menu.addSeparator()
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)

    def _build_import_tab(self) -> None:
        layout = QVBoxLayout(self.import_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        controls_layout = QGridLayout()
        controls_layout.setContentsMargins(0, 0, 0, 0)
        controls_layout.setHorizontalSpacing(8)
        controls_layout.setVerticalSpacing(6)

        self.folder_edit = QLineEdit()
        self.folder_edit.setPlaceholderText("Choose an input folder with GIS vector data")
        if self.current_settings.last_folder:
            self.folder_edit.setText(self.current_settings.last_folder)
        self.folder_edit.textChanged.connect(self.on_folder_changed)
        controls_layout.addWidget(self.folder_edit, 0, 0, 1, 3)

        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self.browse_folder)
        controls_layout.addWidget(self.browse_button, 0, 3)

        self.scan_button = QPushButton(REBUILD_ARCHIVE_LABEL)
        self.scan_button.clicked.connect(self.start_scan)
        controls_layout.addWidget(self.scan_button, 1, 0)

        self.load_existing_button = QPushButton(LOAD_ARCHIVE_LABEL)
        self.load_existing_button.clicked.connect(self.load_existing_catalog)
        controls_layout.addWidget(self.load_existing_button, 1, 1)

        self.reset_data_button = QPushButton("Reset All Data")
        self.reset_data_button.clicked.connect(self.reset_all_data)
        controls_layout.addWidget(self.reset_data_button, 1, 2, 1, 2)
        controls_layout.setColumnStretch(0, 1)
        controls_layout.setColumnStretch(1, 1)
        controls_layout.setColumnStretch(2, 1)
        layout.addLayout(controls_layout)

        self.simplify_import_names_checkbox = QCheckBox("Simplify long dataset names on import")
        self.simplify_import_names_checkbox.setChecked(False)
        self.simplify_import_names_checkbox.setToolTip(
            "Shorten long technical source names during import and move the omitted source naming context into "
            "Description. Existing manual Name/Description edits are kept."
        )
        layout.addWidget(self.simplify_import_names_checkbox)

        self.import_summary = QLabel("No folder loaded.")
        layout.addWidget(self.import_summary)

        self.import_progress = QProgressBar()
        self.import_progress.setRange(0, 100)
        layout.addWidget(self.import_progress)

        self.import_table = QTableWidget(0, 6)
        self.import_table.setHorizontalHeaderLabels(["Name", "Format", "Geometry", "Features", "Styling", "Source"])
        self.import_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.import_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.import_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.import_table.setSortingEnabled(True)
        self.import_table.horizontalHeader().setSortIndicatorShown(True)
        self.import_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.import_table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Stretch)
        layout.addWidget(self.import_table, 1)

        self.import_log_note = QLabel(
            "Background activity is written to the global activity log. "
            "Use Logs in the top-right corner to open it, and watch the button for live progress."
        )
        self.import_log_note.setWordWrap(True)
        layout.addWidget(self.import_log_note)
        self._update_folder_actions()

    def _build_review_datasets_tab(self) -> None:
        layout = QVBoxLayout(self.review_datasets_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        self.review_dataset_splitter = CanvasSplitter(Qt.Horizontal)
        self.review_dataset_splitter.setChildrenCollapsible(False)
        layout.addWidget(self.review_dataset_splitter, 1)

        browser_host = QWidget()
        browser_host.setMinimumWidth(180)
        browser_host.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        browser_layout = QVBoxLayout(browser_host)
        browser_layout.setContentsMargins(0, 0, 0, 0)
        browser_layout.setSpacing(8)

        self.review_dataset_filter_edit = QLineEdit()
        self.review_dataset_filter_edit.setPlaceholderText("Filter datasets by name, group, format, geometry or source path")
        self.review_dataset_filter_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.review_dataset_filter_edit.textChanged.connect(self.refresh_dataset_browser_table)
        browser_layout.addWidget(self.review_dataset_filter_edit)

        self.review_dataset_table = QTableWidget(0, 3)
        self.review_dataset_table.setHorizontalHeaderLabels(["Dataset", "Group", "Format"])
        self.review_dataset_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.review_dataset_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.review_dataset_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.review_dataset_table.setSortingEnabled(False)
        self.review_dataset_table.verticalHeader().setVisible(False)
        self.review_dataset_table.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.review_dataset_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.review_dataset_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.review_dataset_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.review_dataset_table.itemSelectionChanged.connect(self.on_review_dataset_selection_changed)
        browser_layout.addWidget(self.review_dataset_table, 1)

        self.review_dataset_list_note = QLabel(
            "This browser lists all datasets without batch-selection checkboxes. Use the filter above to narrow the list."
        )
        self.review_dataset_list_note.setWordWrap(True)
        self.review_dataset_list_note.setMinimumWidth(0)
        self.review_dataset_list_note.setSizePolicy(QSizePolicy.Ignored, QSizePolicy.Preferred)
        browser_layout.addWidget(self.review_dataset_list_note)

        self.review_dataset_splitter.addWidget(browser_host)

        details_host = QWidget()
        details_host.setMinimumWidth(260)
        details_host.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        details_layout = QVBoxLayout(details_host)
        details_layout.setContentsMargins(0, 0, 0, 0)
        details_layout.setSpacing(8)

        self.dataset_details_group_box = QGroupBox("Selected dataset")
        dataset_form = QFormLayout(self.dataset_details_group_box)
        self.dataset_name_edit = QLineEdit()
        dataset_form.addRow("Name", self.dataset_name_edit)

        self.dataset_description_edit = QTextEdit()
        self.dataset_description_edit.setMinimumHeight(72)
        self.dataset_description_edit.setMaximumHeight(96)
        dataset_form.addRow("Description", self.dataset_description_edit)

        self.dataset_group_combo = QComboBox()
        self.dataset_group_combo.currentIndexChanged.connect(self._on_dataset_group_changed)
        dataset_form.addRow("Group", self.dataset_group_combo)

        self.visibility_checkbox = QCheckBox("Visible on map")
        self.include_export_checkbox = QCheckBox("Include in export")
        self.visibility_checkbox.toggled.connect(self._on_visibility_checkbox_toggled)
        self.include_export_checkbox.toggled.connect(self._on_include_export_checkbox_toggled)
        dataset_form.addRow("", self.visibility_checkbox)
        dataset_form.addRow("", self.include_export_checkbox)

        self.source_path_label = QLabel("-")
        self.source_path_label.setWordWrap(True)
        dataset_form.addRow("Source", self.source_path_label)

        self.geometry_label = QLabel("-")
        dataset_form.addRow("Geometry", self.geometry_label)

        self.feature_count_label = QLabel("-")
        dataset_form.addRow("Features", self.feature_count_label)

        self.source_style_label = QLabel("-")
        self.source_style_label.setWordWrap(True)
        dataset_form.addRow("Source styling", self.source_style_label)

        self.ai_title_label = QLabel("-")
        self.ai_title_label.setWordWrap(True)
        dataset_form.addRow("AI title", self.ai_title_label)

        self.ai_group_label = QLabel("-")
        dataset_form.addRow("AI group", self.ai_group_label)

        self.ai_description_box = QPlainTextEdit()
        self.ai_description_box.setReadOnly(True)
        self.ai_description_box.setMinimumHeight(72)
        self.ai_description_box.setMaximumHeight(96)
        dataset_form.addRow("AI description", self.ai_description_box)

        self.transfer_ai_selected_button = QPushButton("Transfer AI to Name + Description")
        self.transfer_ai_selected_button.clicked.connect(self.use_ai_for_selected_dataset)
        self.transfer_ai_selected_button.setMaximumWidth(220)

        self.save_dataset_button = QPushButton("Save Changes")
        self.save_dataset_button.clicked.connect(self.save_selected_dataset)
        self.save_dataset_button.setMaximumWidth(150)

        dataset_button_host = QWidget()
        dataset_button_layout = QHBoxLayout(dataset_button_host)
        dataset_button_layout.setContentsMargins(0, 2, 0, 2)
        dataset_button_layout.setSpacing(8)
        dataset_button_layout.addStretch(1)
        dataset_button_layout.addWidget(self.transfer_ai_selected_button)
        dataset_button_layout.addWidget(self.save_dataset_button)
        dataset_button_layout.addStretch(1)
        dataset_form.addRow("", dataset_button_host)

        details_layout.addWidget(self.dataset_details_group_box)

        navigation_host = QWidget()
        navigation_layout = QHBoxLayout(navigation_host)
        navigation_layout.setContentsMargins(0, 0, 0, 0)
        navigation_layout.setSpacing(8)
        navigation_layout.addStretch(1)

        self.dataset_nav_first_button = QPushButton("First")
        self.dataset_nav_first_button.clicked.connect(self.select_first_review_dataset)
        navigation_layout.addWidget(self.dataset_nav_first_button)

        self.dataset_nav_back_button = QPushButton("Back")
        self.dataset_nav_back_button.clicked.connect(self.select_previous_review_dataset)
        navigation_layout.addWidget(self.dataset_nav_back_button)

        self.dataset_nav_next_button = QPushButton("Next")
        self.dataset_nav_next_button.clicked.connect(self.select_next_review_dataset)
        navigation_layout.addWidget(self.dataset_nav_next_button)

        self.dataset_nav_last_button = QPushButton("Last")
        self.dataset_nav_last_button.clicked.connect(self.select_last_review_dataset)
        navigation_layout.addWidget(self.dataset_nav_last_button)
        navigation_layout.addStretch(1)

        details_layout.addWidget(navigation_host)
        details_layout.addStretch(1)

        self.review_dataset_splitter.addWidget(details_host)
        self.review_dataset_splitter.setStretchFactor(0, 3)
        self.review_dataset_splitter.setStretchFactor(1, 4)
        QTimer.singleShot(0, self._sync_review_dataset_splitter_sizes)
        self._update_dataset_navigation_buttons()

    def _build_info_sources_tab(self) -> None:
        layout = QVBoxLayout(self.info_sources_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(10)

        self.info_sources_intro_label = QLabel(
            "Use this page to define one checked working set, then run discovery, grouping, and batch changes on that same set."
        )
        self.info_sources_intro_label.setWordWrap(True)
        layout.addWidget(self.info_sources_intro_label)

        self.selection_group_box = QGroupBox("2. Choose datasets for batch work")
        selection_layout = QVBoxLayout(self.selection_group_box)
        selection_layout.setSpacing(8)
        self.selection_help_label = QLabel(
            "This checked working set drives steps 1, 3, and 4 below. "
            "It does not control the Map tab. "
            "Use the group dropdown when you want the group buttons to target one specific catalog group."
        )
        self.selection_help_label.setWordWrap(True)
        selection_layout.addWidget(self.selection_help_label)

        selection_controls_layout = QHBoxLayout()
        selection_controls_layout.setContentsMargins(0, 0, 0, 0)
        selection_controls_layout.setSpacing(8)

        self.selection_group_combo = QComboBox()
        self.selection_group_combo.setFixedHeight(30)
        self.selection_group_combo.setMinimumWidth(240)
        self.selection_group_combo.setMaximumWidth(300)
        self.selection_group_combo.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.selection_group_combo.setStyleSheet("QComboBox { padding-left: 8px; }")
        self.selection_group_combo.currentIndexChanged.connect(self._on_selection_group_changed)

        self.show_all_button = QPushButton("Select All")
        self.show_all_button.clicked.connect(lambda: self.set_all_checked(True))
        selection_controls_layout.addWidget(self.show_all_button)

        self.hide_all_button = QPushButton("Clear All")
        self.hide_all_button.clicked.connect(lambda: self.set_all_checked(False))
        selection_controls_layout.addWidget(self.hide_all_button)

        self.show_group_button = QPushButton("Add Group")
        self.show_group_button.clicked.connect(lambda: self.set_selected_group_checked(True))
        selection_controls_layout.addWidget(self.show_group_button)

        self.hide_group_button = QPushButton("Clear Group")
        self.hide_group_button.clicked.connect(lambda: self.set_selected_group_checked(False))
        selection_controls_layout.addWidget(self.hide_group_button)
        selection_controls_layout.addStretch(1)
        selection_controls_layout.addWidget(QLabel("Group"))
        selection_controls_layout.addWidget(self.selection_group_combo)
        selection_layout.addLayout(selection_controls_layout)

        self.selection_scope_status_label = QLabel("No project loaded.")
        self.selection_scope_status_label.setWordWrap(True)
        selection_layout.addWidget(self.selection_scope_status_label)
        layout.addWidget(self.selection_group_box)

        self.info_sources_splitter = CanvasSplitter(Qt.Horizontal)
        self.info_sources_splitter.setChildrenCollapsible(False)
        layout.addWidget(self.info_sources_splitter, 1)

        discovery_host = QWidget()
        discovery_layout = QVBoxLayout(discovery_host)
        discovery_layout.setContentsMargins(0, 0, 0, 0)
        discovery_layout.setSpacing(8)

        self.review_actions_group_box = QGroupBox("1. Discover info and sources")
        review_actions_box_layout = QVBoxLayout(self.review_actions_group_box)
        self.review_actions_intro_label = QLabel(
            "Run one of these discovery steps on the checked working set from step 2. Nothing here changes source files; "
            "it updates GRASP's understanding and source suggestions."
        )
        self.review_actions_intro_label.setWordWrap(True)
        review_actions_box_layout.addWidget(self.review_actions_intro_label)

        review_actions_layout = QGridLayout()
        self.fast_info_button = QPushButton("Find info (fast)")
        self.fast_info_button.clicked.connect(self.start_fast_info_for_scope)
        review_actions_layout.addWidget(self.fast_info_button, 0, 0, alignment=Qt.AlignCenter)

        self.run_ai_sources_button = QPushButton("Find info (AI)")
        self.run_ai_sources_button.clicked.connect(self.start_ai_for_scope)
        review_actions_layout.addWidget(self.run_ai_sources_button, 0, 1, alignment=Qt.AlignCenter)

        self.find_sources_button = QPushButton("Find sources")
        self.find_sources_button.clicked.connect(self.start_sources_for_scope)
        review_actions_layout.addWidget(self.find_sources_button, 1, 0, 1, 2, alignment=Qt.AlignCenter)
        review_actions_layout.setColumnStretch(0, 1)
        review_actions_layout.setColumnStretch(1, 1)
        review_actions_box_layout.addLayout(review_actions_layout)

        self.review_actions_note = QLabel(
            "Find info (fast): local first-pass, no external AI.\n"
            "Find info (AI): updates AI title, AI description, and AI group suggestion.\n"
            "Find sources: keeps the current understanding and refreshes likely external sources only.\n"
            "All three actions use the checked working set defined in step 2."
        )
        self.review_actions_note.setWordWrap(True)
        review_actions_box_layout.addWidget(self.review_actions_note)
        discovery_layout.addWidget(self.review_actions_group_box)

        self.review_job_group_box = QGroupBox("Current run")
        review_job_layout = QVBoxLayout(self.review_job_group_box)
        self.review_job_status = QLabel("No dataset processing job running.")
        self.review_job_status.setWordWrap(True)
        review_job_layout.addWidget(self.review_job_status)

        self.review_visibility_note = QLabel(
            "The checked working set is shared with the checkboxes in Datasets overview. "
            "Visible on map is controlled separately in Review datasets. On import, the app auto-enables map visibility for up to "
            f"{MAX_AUTO_VISIBLE_DATASETS} smaller layers (max {MAX_AUTO_VISIBLE_FEATURES} features each) "
            "and leaves the rest off to keep the map responsive."
        )
        self.review_visibility_note.setWordWrap(True)

        self.review_progress = QProgressBar()
        self.review_progress.setRange(0, 100)
        self.review_progress.setValue(0)
        review_job_layout.addWidget(self.review_progress)
        review_job_layout.addWidget(self.review_visibility_note)
        discovery_layout.addWidget(self.review_job_group_box)
        discovery_layout.addStretch(1)
        self.info_sources_splitter.addWidget(discovery_host)

        batch_host = QWidget()
        batch_layout = QVBoxLayout(batch_host)
        batch_layout.setContentsMargins(0, 0, 0, 0)
        batch_layout.setSpacing(8)

        self.grouping_group_box = QGroupBox("3. Organize checked datasets")
        grouping_layout = QGridLayout(self.grouping_group_box)
        self.new_group_button = QPushButton("New Group")
        self.new_group_button.clicked.connect(self.create_group)
        grouping_layout.addWidget(self.new_group_button, 0, 0, alignment=Qt.AlignCenter)

        self.apply_group_button = QPushButton("Apply Suggested Group")
        self.apply_group_button.clicked.connect(self.apply_suggested_group)
        grouping_layout.addWidget(self.apply_group_button, 0, 1, alignment=Qt.AlignCenter)

        self.regroup_button = QPushButton("AI Regroup...")
        self.regroup_button.clicked.connect(self.start_regroup_for_scope)
        grouping_layout.addWidget(self.regroup_button, 1, 0, alignment=Qt.AlignCenter)

        self.reset_groups_button = QPushButton("Reset Groups")
        self.reset_groups_button.clicked.connect(self.reset_groups_for_scope)
        grouping_layout.addWidget(self.reset_groups_button, 1, 1, alignment=Qt.AlignCenter)
        grouping_layout.setColumnStretch(0, 1)
        grouping_layout.setColumnStretch(1, 1)
        self.grouping_help_label = QLabel(
            "Apply Suggested Group uses the current AI group suggestion for each dataset. "
            "AI Regroup prepares a proposal first so you can review it before anything is applied. "
            "These actions use the checked working set from step 2."
        )
        self.grouping_help_label.setWordWrap(True)
        grouping_layout.addWidget(self.grouping_help_label, 2, 0, 1, 2)
        batch_layout.addWidget(self.grouping_group_box)

        self.dataset_actions_group_box = QGroupBox("4. Apply batch changes")
        dataset_actions_layout = QGridLayout(self.dataset_actions_group_box)
        self.fill_ai_fields_button = QPushButton("Fill Empty Fields from AI")
        self.fill_ai_fields_button.clicked.connect(self.fill_checked_user_fields_from_ai)
        dataset_actions_layout.addWidget(self.fill_ai_fields_button, 0, 0, alignment=Qt.AlignCenter)

        self.make_visible_button = QPushButton("Make visible in maps")
        self.make_visible_button.clicked.connect(self.make_checked_visible_in_maps)
        dataset_actions_layout.addWidget(self.make_visible_button, 0, 1, alignment=Qt.AlignCenter)

        self.hide_from_maps_button = QPushButton("Hide from maps")
        self.hide_from_maps_button.clicked.connect(self.hide_checked_from_maps)
        dataset_actions_layout.addWidget(self.hide_from_maps_button, 1, 0, alignment=Qt.AlignCenter)

        self.include_in_report_button = QPushButton("Include in export")
        self.include_in_report_button.clicked.connect(self.include_checked_in_report)
        dataset_actions_layout.addWidget(self.include_in_report_button, 1, 1, alignment=Qt.AlignCenter)

        self.generate_styles_button = QPushButton("Generate Styles")
        self.generate_styles_button.clicked.connect(self.start_style_for_scope)
        dataset_actions_layout.addWidget(self.generate_styles_button, 2, 0, alignment=Qt.AlignCenter)

        self.exclude_from_report_button = QPushButton("Exclude from export")
        self.exclude_from_report_button.clicked.connect(self.exclude_checked_from_report)
        dataset_actions_layout.addWidget(self.exclude_from_report_button, 2, 1, alignment=Qt.AlignCenter)

        self.export_gpkg_button = QPushButton("Export GeoPackage")
        self.export_gpkg_button.clicked.connect(self.export_gpkg)
        dataset_actions_layout.addWidget(self.export_gpkg_button, 3, 0, 1, 2, alignment=Qt.AlignCenter)
        dataset_actions_layout.setColumnStretch(0, 1)
        dataset_actions_layout.setColumnStretch(1, 1)
        self.dataset_actions_help_label = QLabel(
            "These actions affect only the checked datasets. They update GRASP metadata, grouping, and export or map flags; "
            "the source files stay untouched. Export GeoPackage writes the current included dataset set to a packaged output."
        )
        self.dataset_actions_help_label.setWordWrap(True)
        dataset_actions_layout.addWidget(self.dataset_actions_help_label, 4, 0, 1, 2)
        batch_layout.addWidget(self.dataset_actions_group_box)
        batch_layout.addStretch(1)
        self.info_sources_splitter.addWidget(batch_host)
        self.info_sources_splitter.setStretchFactor(0, 1)
        self.info_sources_splitter.setStretchFactor(1, 1)
        self._configure_manage_data_buttons(
            [
                self.fast_info_button,
                self.run_ai_sources_button,
                self.find_sources_button,
                self.show_all_button,
                self.hide_all_button,
                self.show_group_button,
                self.hide_group_button,
                self.new_group_button,
                self.apply_group_button,
                self.regroup_button,
                self.reset_groups_button,
                self.fill_ai_fields_button,
                self.make_visible_button,
                self.hide_from_maps_button,
                self.include_in_report_button,
                self.generate_styles_button,
                self.exclude_from_report_button,
                self.export_gpkg_button,
            ]
        )
        self._configure_compact_manage_data_buttons(
            [
                self.show_all_button,
                self.hide_all_button,
                self.show_group_button,
                self.hide_group_button,
            ]
        )
        self._populate_selection_group_combo()
        QTimer.singleShot(0, self._sync_info_sources_splitter_sizes)

    def _build_datasets_overview_tab(self) -> None:
        layout = QVBoxLayout(self.datasets_overview_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        self.datasets_group_box = QGroupBox("Datasets")
        datasets_group_layout = QVBoxLayout(self.datasets_group_box)

        self.tree = DatasetTreeWidget()
        self.tree.setHeaderLabels(["Datasets"])
        self.tree.setDragDropMode(QTreeWidget.InternalMove)
        self.tree.setSelectionMode(QTreeWidget.SingleSelection)
        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        self.tree.itemChanged.connect(self.on_tree_item_changed)
        self.tree.orderingChanged.connect(self.on_tree_order_changed)
        datasets_group_layout.addWidget(self.tree, 1)

        self.datasets_help_note = QLabel(
            "Tip: Drag datasets between groups in the overview below to reorganize them manually. "
            "Checkboxes here define the batch-selection scope used by Manage data."
        )
        self.datasets_help_note.setWordWrap(True)
        datasets_group_layout.addWidget(self.datasets_help_note)
        layout.addWidget(self.datasets_group_box, 1)

    def _build_map_tab(self) -> None:
        layout = QVBoxLayout(self.map_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        self.map_controls_layout = QHBoxLayout()
        self.map_controls_layout.setContentsMargins(0, 0, 0, 0)
        self.map_controls_layout.setSpacing(8)

        self.refresh_map_button = QPushButton("Refresh Map")
        self.refresh_map_button.clicked.connect(self.refresh_map)
        self.map_controls_layout.addWidget(self.refresh_map_button, 0, Qt.AlignLeft)
        self.map_controls_layout.addStretch(1)

        self.map_scope_label = QLabel("Scope")
        self.map_controls_layout.addWidget(self.map_scope_label, 0, Qt.AlignRight)
        self.map_scope_combo = QComboBox()
        self.map_scope_combo.addItem("Visible on map", "visible")
        self.map_scope_combo.addItem("Show all", "all")
        self.map_scope_combo.currentIndexChanged.connect(lambda _index: self.refresh_map())
        self.map_controls_layout.addWidget(self.map_scope_combo, 0, Qt.AlignRight)
        layout.addLayout(self.map_controls_layout)

        self.map_summary = QLabel("No project loaded.")
        self.map_summary.setWordWrap(True)
        layout.addWidget(self.map_summary)

        self.map_style_note = QLabel(
            "Generate styles in Manage data applies only to the checked working set. "
            "The Map tab can still show more layers depending on the map scope above. "
            "If possible source styling is detected during import, GRASP warns before generating new AI-driven styling."
        )
        self.map_style_note.setWordWrap(True)
        layout.addWidget(self.map_style_note)

        self.map_view_host = QWidget()
        self.map_view_layout = QVBoxLayout(self.map_view_host)
        self.map_view_layout.setContentsMargins(0, 0, 0, 0)
        self.map_view_layout.setSpacing(0)
        self.map_view = self._build_map_placeholder()
        self.map_view_layout.addWidget(self.map_view, 1)
        layout.addWidget(self.map_view_host, 1)

    def _build_settings_tab(self) -> None:
        layout = QVBoxLayout(self.settings_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        intro = QLabel(
            f"{APP_DISPLAY_NAME} helps you retrieve, analyse, style and package GIS datasets. "
            "Choose the AI and live-search settings used when the app builds dataset understanding and looks for likely external sources."
        )
        intro.setWordWrap(True)
        layout.addWidget(intro)

        self.settings_model_label = QLabel()
        self.settings_model_label.setWordWrap(True)
        layout.addWidget(self.settings_model_label)

        self.ai_settings_group_box = QGroupBox("AI Settings")
        ai_group_layout = QVBoxLayout(self.ai_settings_group_box)
        ai_intro = QLabel(
            "Settings used for manual OpenAI-based title, description, grouping, and source ranking. "
            "Set Managed data language when you know the catalog language; leave it as Not set when the model should infer that from the data."
        )
        ai_intro.setWordWrap(True)
        ai_group_layout.addWidget(ai_intro)

        ai_form = QFormLayout()
        self.settings_model_combo = QComboBox()
        self.settings_model_combo.setEditable(True)
        self.settings_model_combo.addItems(MODEL_OPTIONS)
        ai_form.addRow("OpenAI model", self.settings_model_combo)

        self.settings_data_language_combo = QComboBox()
        self.settings_data_language_combo.addItem(MANAGED_DATA_LANGUAGE_NOT_SET_LABEL, "")
        for language in MANAGED_DATA_LANGUAGE_OPTIONS:
            self.settings_data_language_combo.addItem(language, language)
        ai_form.addRow("Managed data language", self.settings_data_language_combo)

        self.settings_api_key_edit = QLineEdit()
        self.settings_api_key_edit.setEchoMode(QLineEdit.Password)
        self.settings_api_key_edit.setPlaceholderText("Optional if OPENAI_API_KEY is already set in the environment")
        ai_form.addRow("API key", self.settings_api_key_edit)

        self.settings_endpoint_edit = QLineEdit()
        ai_form.addRow("Endpoint", self.settings_endpoint_edit)

        self.settings_timeout_edit = QLineEdit()
        ai_form.addRow("OpenAI timeout (s)", self.settings_timeout_edit)

        self.settings_failures_edit = QLineEdit()
        ai_form.addRow("OpenAI failover threshold", self.settings_failures_edit)
        ai_group_layout.addLayout(ai_form)

        self.ai_context_group_box = QGroupBox("AI Classification Context")
        ai_context_layout = QVBoxLayout(self.ai_context_group_box)
        ai_context_intro = QLabel(
            "Choose which dataset clues are sent to OpenAI during manual AI runs in Manage data. "
            "Keep this lean to reduce token use. Search-based enrichment can add more evidence later. "
            "Set Managed data language above when you know the primary language of the catalog; leave it as Not set when the model should infer that itself."
        )
        ai_context_intro.setWordWrap(True)
        ai_context_layout.addWidget(ai_context_intro)

        self.settings_context_source_name_checkbox = QCheckBox("Include file name")
        ai_context_layout.addWidget(self.settings_context_source_name_checkbox)

        self.settings_context_layer_name_checkbox = QCheckBox("Include layer name")
        ai_context_layout.addWidget(self.settings_context_layer_name_checkbox)

        self.settings_context_column_names_checkbox = QCheckBox("Include column names")
        ai_context_layout.addWidget(self.settings_context_column_names_checkbox)

        self.settings_context_sample_values_checkbox = QCheckBox("Include sample values")
        ai_context_layout.addWidget(self.settings_context_sample_values_checkbox)

        self.settings_context_geometry_checkbox = QCheckBox("Include geometry type")
        ai_context_layout.addWidget(self.settings_context_geometry_checkbox)

        self.settings_context_feature_count_checkbox = QCheckBox("Include feature count")
        ai_context_layout.addWidget(self.settings_context_feature_count_checkbox)

        self.settings_context_bbox_checkbox = QCheckBox("Include bounding box")
        ai_context_layout.addWidget(self.settings_context_bbox_checkbox)

        ai_group_layout.addWidget(self.ai_context_group_box)
        layout.addWidget(self.ai_settings_group_box)

        self.search_settings_group_box = QGroupBox("Search Settings")
        search_group_layout = QVBoxLayout(self.search_settings_group_box)
        search_intro = QLabel("Settings used for live web lookup of likely dataset sources and mirrors.")
        search_intro.setWordWrap(True)
        search_group_layout.addWidget(search_intro)

        search_form = QFormLayout()
        self.settings_search_timeout_edit = QLineEdit()
        search_form.addRow("Search timeout (s)", self.settings_search_timeout_edit)

        self.settings_search_failures_edit = QLineEdit()
        search_form.addRow("Search failover threshold", self.settings_search_failures_edit)

        self.settings_search_candidates_edit = QLineEdit()
        search_form.addRow("Search target candidates", self.settings_search_candidates_edit)
        search_group_layout.addLayout(search_form)
        layout.addWidget(self.search_settings_group_box)

        button_row = QHBoxLayout()
        save_settings_button = QPushButton("Save Settings")
        save_settings_button.clicked.connect(self.save_settings)
        button_row.addWidget(save_settings_button)
        button_row.addStretch(1)
        layout.addLayout(button_row)
        layout.addStretch(1)

        self._apply_settings_to_form(self.current_settings)

    def _build_about_tab(self) -> None:
        layout = QVBoxLayout(self.about_tab)
        layout.setContentsMargins(TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX, TAB_PAGE_MARGIN_PX)
        layout.setSpacing(8)

        self.about_header_label = QLabel(APP_DISPLAY_NAME)
        self.about_header_label.setStyleSheet("font-size: 22px; font-weight: 700; color: #4d4029;")
        layout.addWidget(self.about_header_label)

        self.about_acronym_label = QLabel(f"{APP_ACRONYM} stands for: {APP_TAGLINE}")
        self.about_acronym_label.setWordWrap(True)
        layout.addWidget(self.about_acronym_label)

        self.about_tagline_label = QLabel(APP_TAGLINE)
        self.about_tagline_label.setWordWrap(True)
        self.about_tagline_label.setStyleSheet("font-size: 14px; color: #6a5533;")
        layout.addWidget(self.about_tagline_label)

        self.about_illustration_path = ABOUT_ILLUSTRATION_PATH
        self.about_icon_path = APP_ICON_PATH
        self.about_illustration_label = QLabel()
        self.about_illustration_label.setObjectName("AboutIllustration")
        self.about_illustration_label.setAlignment(Qt.AlignCenter)
        self.about_illustration_label.setMinimumHeight(220)
        self.about_illustration_label.setStyleSheet(
            "background-color: #fbf6ee; border: 1px solid #d4c0a2; border-radius: 16px; padding: 10px;"
        )
        illustration_pixmap = QPixmap(str(self.about_illustration_path))
        if illustration_pixmap.isNull():
            self.about_illustration_label.setText(
                "Playful GRASP illustration: scattered dataset files are funneled into a GPKG bag."
            )
            self.about_illustration_label.setWordWrap(True)
        else:
            self.about_illustration_label.setPixmap(
                illustration_pixmap.scaled(560, 220, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            )
        layout.addWidget(self.about_illustration_label)

        self.about_author_label = QLabel(f"Created by {APP_AUTHOR}")
        self.about_author_label.setWordWrap(True)
        layout.addWidget(self.about_author_label)

        self.about_purpose_label = QLabel(
            "Why I made it: GIS work can involve an overwhelming number of datasets that need to be "
            "compiled into a unified store for further use or distribution. Manual handling is just "
            "not realistic at that scale, so GRASP is a desktop application for working through folders "
            "of GIS vector files such as Shapefile, GeoPackage and GeoParquet datasets."
        )
        self.about_purpose_label.setWordWrap(True)
        layout.addWidget(self.about_purpose_label)

        self.about_mission_label = QLabel(APP_MISSION)
        self.about_mission_label.setWordWrap(True)
        layout.addWidget(self.about_mission_label)

        self.about_capabilities_label = QLabel(
            "What it does:\n"
            "- scans folders for GIS datasets\n"
            "- profiles and enriches datasets with AI-generated names and descriptions\n"
            "- helps users group datasets into rational categories\n"
            "- previews layers in a lightweight desktop map\n"
            "- generates styling from names and descriptions\n"
            "- exports a packaged GeoPackage with metadata and QGIS project information\n"
            "- helps assemble a more usable, unified dataset package for follow-on work"
        )
        self.about_capabilities_label.setWordWrap(True)
        layout.addWidget(self.about_capabilities_label)

        self.about_note_label = QLabel(
            "Approach and trade-off: the upside is faster processing and AI assistance that can help fill "
            "gaps when source metadata is incomplete. The downside is that automation may still miss details, "
            "so human review is still needed before distribution."
        )
        self.about_note_label.setWordWrap(True)
        layout.addWidget(self.about_note_label)

        self.about_links_label = QLabel(
            f'Links: <a href="{APP_LINKEDIN_URL}">LinkedIn profile</a> | '
            f'<a href="{APP_REPOSITORY_URL}">GitHub repository</a>'
        )
        self.about_links_label.setWordWrap(True)
        self.about_links_label.setOpenExternalLinks(True)
        self.about_links_label.setTextFormat(Qt.RichText)
        layout.addWidget(self.about_links_label)

        layout.addStretch(1)

    def browse_folder(self) -> None:
        initial_folder = self.folder_edit.text().strip() or self.current_settings.last_folder.strip() or str(Path.home())
        folder = QFileDialog.getExistingDirectory(self, "Choose input folder", initial_folder)
        if folder:
            self.folder_edit.setText(folder)
            self._remember_last_folder(folder)
            self.statusBar().showMessage(f"Selected {folder}")

    def on_folder_changed(self, _value: str) -> None:
        self._update_folder_actions()

    def load_existing_catalog(self) -> None:
        folder = self.folder_edit.text().strip()
        if not folder:
            QMessageBox.information(self, "Choose folder", "Select a folder first.")
            return
        self._remember_last_folder(folder)
        workspace = ensure_workspace(folder)
        if not workspace.db_path.exists():
            QMessageBox.information(self, "Catalog missing", "No local catalog exists yet for this folder. Run a scan first.")
            return
        self._set_workspace(folder)
        self.append_activity_log(f"Loaded existing catalog from {workspace.db_path}", activity=LOAD_ARCHIVE_LABEL)
        self.refresh_all_views()

    def start_scan(self) -> None:
        folder = self.folder_edit.text().strip()
        if not folder:
            QMessageBox.information(self, "Choose folder", "Select a folder first.")
            return
        if not Path(folder).exists():
            QMessageBox.warning(self, "Folder missing", "The selected folder does not exist.")
            return
        self._remember_last_folder(folder)
        self._set_workspace(folder)
        self.import_progress.setValue(0)
        self.append_activity_log(f"Loading datasets from {folder}", activity=REBUILD_ARCHIVE_LABEL)
        existing_records = self.repository.list_datasets() if self.repository is not None else []
        worker = FunctionWorker(self.ingest_service.scan_folder, folder, existing_records)
        progress_token = self._begin_background_activity("Rebuilding archive...", activity=REBUILD_ARCHIVE_LABEL)
        self._active_workers[progress_token] = worker
        worker.signals.status.connect(lambda message: self.append_activity_log(message, activity=REBUILD_ARCHIVE_LABEL))
        worker.signals.progress.connect(self.import_progress.setValue)
        worker.signals.status.connect(lambda message, token=progress_token: self._update_background_activity_status(token, message))
        worker.signals.progress.connect(lambda value, token=progress_token: self._update_background_activity_progress(token, value))
        worker.signals.result.connect(lambda datasets, token=progress_token: self._schedule_scan_result(token, datasets))
        worker.signals.error.connect(lambda message, token=progress_token: self.on_background_error(message, token))
        worker.signals.finished.connect(lambda token=progress_token: self._release_worker(token))
        self.thread_pool.start(worker)

    def _schedule_scan_result(self, token: int, datasets) -> None:
        QTimer.singleShot(0, lambda token=token, datasets=datasets: self._complete_scan_result(token, datasets))

    def _complete_scan_result(self, token: int, datasets) -> None:
        try:
            self._update_background_activity_status(token, "Finalizing loaded datasets in the catalog.")
            self.on_scan_result(datasets)
            self._finish_background_activity(token, "Rebuild archive finished.")
            self.statusBar().showMessage("Rebuild archive finished.", 5000)
        except Exception:
            self.on_background_error(traceback.format_exc(), token)

    def on_scan_result(self, datasets) -> None:
        if self.repository is None:
            return
        sync_summary = self.repository.replace_datasets(datasets)
        self.append_activity_log(f"Persisted {len(datasets)} dataset(s) to local catalog.", activity=REBUILD_ARCHIVE_LABEL)
        simplified_count = self._apply_import_name_simplification(datasets)
        if simplified_count:
            self.append_activity_log(
                f"Applied simplified import names to {simplified_count} dataset(s); source naming context was moved into Description.",
                activity=REBUILD_ARCHIVE_LABEL,
            )
        source_style_count = sum(1 for dataset in datasets if dataset.has_source_style)
        if source_style_count:
            self.append_activity_log(
                f"Detected possible source styling for {source_style_count} dataset(s). Review Source styling before using Generate Styles.",
                activity=REBUILD_ARCHIVE_LABEL,
            )
        if sync_summary["reused_ids"]:
            self.append_activity_log(
                f"Reused {len(sync_summary['reused_ids'])} unchanged dataset(s) from the existing catalog.",
                activity=REBUILD_ARCHIVE_LABEL,
            )
        if sync_summary["removed_ids"]:
            self.append_activity_log(
                f"Removed {len(sync_summary['removed_ids'])} dataset(s) no longer present in the source folder.",
                activity=REBUILD_ARCHIVE_LABEL,
            )
        self.append_activity_log("Applying loaded datasets to the catalog and refreshing views.", activity=REBUILD_ARCHIVE_LABEL)
        self.refresh_all_views()
        dataset_ids = sync_summary["changed_ids"]
        if dataset_ids:
            self.append_activity_log(
                f"{len(dataset_ids)} new or changed dataset(s) are ready for Find info (fast) in Manage data.",
                activity=REBUILD_ARCHIVE_LABEL,
            )
        else:
            self.append_activity_log(
                "No new or changed datasets detected. Existing AI understanding and sources were kept.",
                activity=REBUILD_ARCHIVE_LABEL,
            )

    def _apply_import_name_simplification(self, datasets) -> int:
        if self.repository is None:
            return 0
        if not self.simplify_import_names_checkbox.isChecked():
            return 0
        updated = 0
        for dataset in datasets:
            if dataset.display_name_user.strip():
                continue
            source_name, source_kind = self._import_name_source(dataset)
            suggestion = suggest_simplified_dataset_name(source_name, source_kind=source_kind)
            if suggestion is None:
                continue
            new_description = dataset.description_user.strip()
            if not new_description:
                new_description = suggestion.description_note
            self.repository.save_dataset_user_fields(
                dataset.dataset_id,
                display_name_user=suggestion.display_name,
                description_user=new_description,
                visibility=dataset.visibility,
                include_in_export=dataset.include_in_export,
            )
            dataset.display_name_user = suggestion.display_name
            dataset.description_user = new_description
            updated += 1
        return updated

    def _import_name_source(self, dataset) -> tuple[str, str]:
        if dataset.layer_name.strip():
            return dataset.layer_name.strip(), "layer"
        source_name = Path(dataset.source_basename).stem or dataset.source_basename
        return source_name, "file"

    def save_settings(self) -> None:
        try:
            timeout_s = float(self.settings_timeout_edit.text().strip() or str(DEFAULT_OPENAI_TIMEOUT_S))
            max_failures = int(self.settings_failures_edit.text().strip() or "2")
            search_timeout_s = float(self.settings_search_timeout_edit.text().strip() or "4")
            search_max_failures = int(self.settings_search_failures_edit.text().strip() or "1")
            search_target_candidates = int(self.settings_search_candidates_edit.text().strip() or "5")
        except ValueError:
            QMessageBox.warning(
                self,
                "Invalid settings",
                "OpenAI/search timeout fields must be numbers, and failover/candidate fields must be integers.",
            )
            return
        context_flags = [
            self.settings_context_source_name_checkbox.isChecked(),
            self.settings_context_layer_name_checkbox.isChecked(),
            self.settings_context_column_names_checkbox.isChecked(),
            self.settings_context_sample_values_checkbox.isChecked(),
            self.settings_context_geometry_checkbox.isChecked(),
            self.settings_context_feature_count_checkbox.isChecked(),
            self.settings_context_bbox_checkbox.isChecked(),
        ]
        if not any(context_flags):
            QMessageBox.warning(
                self,
                "Invalid settings",
                "Select at least one metadata clue for manual AI classification.",
            )
            return
        self.current_settings = AppSettings(
            openai_model=self.settings_model_combo.currentText().strip() or DEFAULT_OPENAI_MODEL,
            openai_api_key=self.settings_api_key_edit.text().strip(),
            openai_endpoint=self.settings_endpoint_edit.text().strip(),
            managed_data_language=normalize_managed_data_language(self.settings_data_language_combo.currentData()),
            openai_timeout_s=timeout_s,
            openai_max_consecutive_failures=max(1, max_failures),
            classification_include_source_name=self.settings_context_source_name_checkbox.isChecked(),
            classification_include_layer_name=self.settings_context_layer_name_checkbox.isChecked(),
            classification_include_column_names=self.settings_context_column_names_checkbox.isChecked(),
            classification_include_sample_values=self.settings_context_sample_values_checkbox.isChecked(),
            classification_include_geometry_type=self.settings_context_geometry_checkbox.isChecked(),
            classification_include_feature_count=self.settings_context_feature_count_checkbox.isChecked(),
            classification_include_bbox=self.settings_context_bbox_checkbox.isChecked(),
            search_timeout_s=max(0.1, search_timeout_s),
            search_max_consecutive_failures=max(1, search_max_failures),
            search_target_candidates=max(1, search_target_candidates),
            last_folder=self.current_settings.last_folder,
        )
        self.settings_store.save(self.current_settings)
        self._rebuild_ai_services()
        self._update_model_label()
        self.statusBar().showMessage(f"AI settings saved. Active model: {self.current_settings.openai_model}", 5000)

    def reset_all_data(self) -> None:
        folder = self.folder_edit.text().strip()
        if not folder:
            QMessageBox.information(self, "Choose folder", "Select a folder first.")
            return
        if not catalog_exists(folder):
            QMessageBox.information(self, "Nothing to reset", "No existing local catalog was found for this folder.")
            self._update_folder_actions()
            return
        answer = QMessageBox.question(
            self,
            "Reset all data",
            "Delete all locally generated catalog data, cache, exports, logs and settings for this folder?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        workspace = ensure_workspace(folder)
        if workspace.workspace_path.exists():
            shutil.rmtree(workspace.workspace_path, ignore_errors=True)
        self.current_workspace = None
        self.repository = None
        self.ingest_service = None
        self.export_service = None
        self.map_bridge = None
        self.map_channel = None
        self._map_initialized = False
        self._map_page_ready = False
        self._map_refresh_pending = False
        self._background_activity_names.clear()
        self._active_workers.clear()
        self._active_background_progress_token = 0
        self._background_activity_started_at = None
        self._background_heartbeat_timer.stop()
        self.import_table.setRowCount(0)
        self.import_summary.setText("No folder loaded.")
        self.tree.clear()
        self.populate_inspector(None)
        self._populate_selection_group_combo()
        self.log_text.setPlainText("")
        self.map_summary.setText("No project loaded.")
        if WEBENGINE_AVAILABLE and hasattr(self.map_view, "setHtml"):
            self.map_view.setHtml("<html><body style='font-family:Segoe UI,sans-serif;padding:24px;'>No project loaded.</body></html>")
        elif hasattr(self.map_view, "setPlainText"):
            self.map_view.setPlainText("No project loaded.")
        self._update_folder_actions()
        self.statusBar().showMessage("Local catalog data was reset.", 5000)

    def create_group(self) -> None:
        if self.repository is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        name, ok = QInputDialog.getText(self, "Create group", "Group name")
        if ok and name.strip():
            new_group_id = self.repository.create_group(name.strip())
            self.refresh_tree()
            self._populate_selection_group_combo(preferred_group_id=new_group_id)

    def start_ai_selected(self) -> None:
        self.start_ai_for_scope("checked")

    def start_fast_info_for_scope(self, scope: str = "checked") -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Use Select All there when you want to run against the whole catalog.",
            )
            return
        self.append_activity_log(
            "Fast local classification runs without external AI and is intended as a quick first pass.",
            activity="Fast Local Classification",
        )
        self._start_worker_with_refresh(
            self._heuristic_classify_dataset_ids,
            dataset_ids,
            "Fast local classification completed.",
            start_message="Running fast local classification without external AI (max 1 minute)...",
            activity_name="Fast Local Classification",
        )

    def start_ai_for_scope(self, scope: str = "checked") -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Use Select All there when you want to run against the whole catalog.",
            )
            return
        ai_runtime_note = self._ai_runtime_note(len(dataset_ids))
        if ai_runtime_note:
            self.append_activity_log(ai_runtime_note, activity="AI Understanding")
        self._start_worker_with_refresh(
            self._classify_dataset_ids,
            dataset_ids,
            "AI dataset understanding completed.",
            start_message="Finding dataset information with AI, one dataset at a time...",
            activity_name="AI Understanding",
        )

    def start_ai_all(self) -> None:
        self.start_ai_for_scope("all")

    def start_sources_selected(self) -> None:
        self.start_sources_for_scope("checked")

    def start_sources_for_scope(self, scope: str = "checked") -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Use Select All there when you want to run against the whole catalog.",
            )
            return
        self._start_worker_with_refresh(
            self._search_dataset_ids,
            dataset_ids,
            "Source lookup completed.",
            start_message="Finding likely sources from the current dataset understanding...",
            activity_name="Source Refresh",
        )

    def start_sources_all(self) -> None:
        self.start_sources_for_scope("all")

    def apply_suggested_group(self) -> None:
        if self.repository is None:
            return
        dataset_id = self.selected_dataset_id()
        if not dataset_id:
            return
        self.repository.apply_suggested_group(dataset_id)
        self.refresh_all_views()

    def start_regroup_checked(self) -> None:
        self.start_regroup_for_scope("checked")

    def start_regroup_for_scope(self, scope: str = "checked") -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Use Select All there when you want to regroup the whole catalog.",
            )
            return
        target_group_count = self._prompt_group_count(len(dataset_ids), "checked datasets" if scope == "checked" else "all datasets")
        if target_group_count <= 0:
            return
        self._start_regroup_preview_job(
            dataset_ids,
            target_group_count,
            scope_label="checked datasets" if scope == "checked" else "all datasets",
        )

    def start_regroup_all(self) -> None:
        self.start_regroup_for_scope("all")

    def reset_groups_for_scope(self, scope: str = "checked") -> None:
        if not self._ensure_review_job_can_start():
            return
        if self.repository is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Use Select All there when you want to reset the whole catalog.",
            )
            return
        scope_label = "checked datasets" if scope == "checked" else "all datasets"
        answer = QMessageBox.question(
            self,
            "Reset groups",
            f"Move {len(dataset_ids)} {scope_label} back to Ungrouped?\n\n"
            "Any groups left empty afterward will be removed.",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if answer != QMessageBox.Yes:
            return
        changed = self.repository.reset_groups(dataset_ids)
        self.append_activity_log(
            f"Reset groups for {changed} dataset(s) in {scope_label}. Any empty groups were removed.",
            activity="Grouping",
        )
        self.refresh_all_views()
        self.statusBar().showMessage(f"Reset groups for {changed} dataset(s).", 5000)

    def start_style_for_scope(self) -> None:
        if self.repository is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets in step 2 first. Generate Styles uses the current checked working set.",
            )
            return
        if not self._confirm_style_generation_for_dataset_ids(dataset_ids):
            return
        self._start_worker_with_refresh(
            self._style_dataset_ids,
            dataset_ids,
            "Style generation completed.",
            start_message="Generating dataset styles from names and descriptions...",
            activity_name="Generate Styles",
        )

    def set_all_checked(self, checked: bool) -> None:
        if self.repository is None:
            return
        dataset_ids = [dataset.dataset_id for dataset in self._datasets()]
        if checked:
            self._checked_dataset_id_set = set(dataset_ids)
        else:
            self._checked_dataset_id_set.clear()
        self.refresh_tree()

    def set_selected_group_checked(self, checked: bool) -> None:
        if self.repository is None:
            return
        group_id = self._selected_batch_group_id() or self.selected_group_id()
        if not group_id:
            QMessageBox.information(self, "Choose group", "Choose a group in step 2 first.")
            return
        dataset_ids = self._dataset_ids_for_group_id(group_id)
        if not dataset_ids:
            QMessageBox.information(self, "No datasets", "The selected group does not contain any datasets.")
            return
        if checked:
            self._checked_dataset_id_set.update(dataset_ids)
        else:
            self._checked_dataset_id_set.difference_update(dataset_ids)
        self.refresh_tree()

    def _on_selection_group_changed(self, _index: int) -> None:
        self._update_manage_data_scope_summary()

    def set_all_visibility(self, visibility: bool) -> None:
        self.set_all_checked(visibility)

    def set_selected_group_visibility(self, visibility: bool) -> None:
        self.set_selected_group_checked(visibility)

    def save_selected_dataset(self) -> None:
        if self.repository is None:
            return
        dataset_id = self.selected_dataset_id()
        if not dataset_id:
            return
        stored = self.repository.get_dataset(dataset_id)
        self.repository.save_dataset_user_fields(
            dataset_id,
            display_name_user=self.dataset_name_edit.text().strip(),
            description_user=self.dataset_description_edit.toPlainText().strip(),
            visibility=self.visibility_checkbox.isChecked(),
            include_in_export=self.include_export_checkbox.isChecked(),
        )
        selected_group_id = self._selected_dataset_group_id_from_inspector()
        if stored is not None and stored.group_id != selected_group_id:
            self.repository.assign_group(dataset_id, selected_group_id)
        self.refresh_all_views()

    def _on_dataset_group_changed(self, _index: int) -> None:
        if self.repository is None or self._populating_inspector:
            return
        dataset_id = self.selected_dataset_id()
        if not dataset_id:
            return
        stored = self.repository.get_dataset(dataset_id)
        if stored is None:
            return
        selected_group_id = self._selected_dataset_group_id_from_inspector()
        if stored.group_id == selected_group_id:
            return
        self.repository.assign_group(dataset_id, selected_group_id)
        self.refresh_all_views()
        self.statusBar().showMessage(
            f"Assigned dataset to {display_group_name(selected_group_id)}.",
            4000,
        )

    def _on_visibility_checkbox_toggled(self, _checked: bool) -> None:
        self._save_selected_dataset_flags()

    def _on_include_export_checkbox_toggled(self, _checked: bool) -> None:
        self._save_selected_dataset_flags()

    def _save_selected_dataset_flags(self) -> None:
        if self.repository is None or self._populating_inspector:
            return
        dataset_id = self.selected_dataset_id()
        if not dataset_id:
            return
        stored = self.repository.get_dataset(dataset_id)
        if stored is None:
            return
        visibility = self.visibility_checkbox.isChecked()
        include_in_export = self.include_export_checkbox.isChecked()
        if stored.visibility == visibility and stored.include_in_export == include_in_export:
            return
        self.repository.save_dataset_user_fields(
            dataset_id,
            display_name_user=stored.display_name_user,
            description_user=stored.description_user,
            visibility=visibility,
            include_in_export=include_in_export,
        )
        updated = self.repository.get_dataset(dataset_id)
        if updated is not None:
            self.populate_inspector(updated)
        self.refresh_tree()
        self.refresh_map()
        self.statusBar().showMessage("Dataset map/export flags updated.", 4000)

    def fill_checked_user_fields_from_ai(self) -> None:
        if self.repository is None:
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets first.")
            return
        changed = self.repository.fill_empty_user_fields_from_ai(dataset_ids)
        if changed <= 0:
            QMessageBox.information(
                self,
                "Nothing to fill",
                "The checked datasets do not have empty Name/Description fields that can be filled from AI.",
            )
            return
        self.append_activity_log(
            f"Filled empty Name/Description fields from AI for {changed} checked dataset(s).",
            activity="Apply AI Text",
        )
        self.statusBar().showMessage(f"Filled empty Name/Description from AI for {changed} dataset(s).", 5000)
        self.refresh_all_views()

    def make_checked_visible_in_maps(self) -> None:
        self._set_checked_visibility(True)

    def hide_checked_from_maps(self) -> None:
        self._set_checked_visibility(False)

    def _set_checked_visibility(self, visible: bool) -> None:
        if self.repository is None:
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets first.")
            return
        self.repository.set_visibility_for_datasets(dataset_ids, visible)
        if visible:
            self.map_scope_combo.setCurrentIndex(self.map_scope_combo.findData("visible"))
            self.append_activity_log(
                f"Enabled map visibility for {len(dataset_ids)} checked dataset(s).",
                activity="Selection Actions",
            )
            self.statusBar().showMessage(f"Enabled map visibility for {len(dataset_ids)} dataset(s).", 5000)
        else:
            self.append_activity_log(
                f"Disabled map visibility for {len(dataset_ids)} checked dataset(s).",
                activity="Selection Actions",
            )
            self.statusBar().showMessage(f"Disabled map visibility for {len(dataset_ids)} dataset(s).", 5000)
        self.refresh_all_views()

    def include_checked_in_report(self) -> None:
        self._set_checked_export_inclusion(True)

    def exclude_checked_from_report(self) -> None:
        self._set_checked_export_inclusion(False)

    def _set_checked_export_inclusion(self, include_in_export: bool) -> None:
        if self.repository is None:
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets first.")
            return
        self.repository.set_include_in_export_for_datasets(dataset_ids, include_in_export)
        if include_in_export:
            self.append_activity_log(
                f"Included {len(dataset_ids)} checked dataset(s) in export output.",
                activity="Selection Actions",
            )
            self.statusBar().showMessage(f"Included {len(dataset_ids)} dataset(s) in export.", 5000)
        else:
            self.append_activity_log(
                f"Excluded {len(dataset_ids)} checked dataset(s) from export output.",
                activity="Selection Actions",
            )
            self.statusBar().showMessage(f"Excluded {len(dataset_ids)} dataset(s) from export.", 5000)
        self.refresh_all_views()

    def transfer_ai_to_checked(self) -> None:
        self._transfer_ai_to_scope("checked")

    def transfer_ai_to_all(self) -> None:
        self._transfer_ai_to_scope("all")

    def _transfer_ai_to_scope(self, scope: str) -> None:
        if self.repository is None:
            return
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Check one or more datasets first, or use All when you want to transfer AI text everywhere.",
            )
            return
        changed = self.repository.transfer_user_fields_from_ai(dataset_ids)
        if changed <= 0:
            QMessageBox.information(
                self,
                "Nothing to transfer",
                "The chosen datasets do not have AI title or AI description available for transfer.",
            )
            return
        scope_label = "checked" if scope == "checked" else "all"
        self.append_activity_log(
            f"Transferred AI title/description to Name/Description for {changed} {scope_label} dataset(s).",
            activity="Transfer AI Text",
        )
        self.statusBar().showMessage(
            f"Transferred AI title/description for {changed} dataset(s).",
            5000,
        )
        self.refresh_all_views()

    def on_tree_selection_changed(self) -> None:
        dataset = self.selected_dataset()
        self.populate_inspector(dataset)
        if not self._syncing_dataset_selection:
            self._syncing_dataset_selection = True
            try:
                self._set_review_dataset_table_selection(self.selected_dataset_id())
            finally:
                self._syncing_dataset_selection = False
        self._update_dataset_navigation_buttons()

    def on_review_dataset_selection_changed(self) -> None:
        self._update_dataset_navigation_buttons()
        if self._syncing_dataset_selection:
            return
        dataset_id = self.selected_review_dataset_id()
        if not dataset_id:
            return
        self._syncing_dataset_selection = True
        try:
            self._set_tree_current_dataset(dataset_id)
        finally:
            self._syncing_dataset_selection = False

    def selected_review_dataset_id(self) -> str:
        if not hasattr(self, "review_dataset_table"):
            return ""
        selection_model = self.review_dataset_table.selectionModel()
        if selection_model is None:
            return ""
        selected_rows = selection_model.selectedRows()
        if not selected_rows:
            return ""
        row = selected_rows[0].row()
        if row < 0 or row >= len(self._dataset_browser_row_ids):
            return ""
        return self._dataset_browser_row_ids[row]

    def _current_review_dataset_row(self) -> int:
        if not hasattr(self, "review_dataset_table"):
            return -1
        selection_model = self.review_dataset_table.selectionModel()
        if selection_model is None:
            return -1
        selected_rows = selection_model.selectedRows()
        if not selected_rows:
            return -1
        return selected_rows[0].row()

    def _set_tree_current_dataset(self, dataset_id: str) -> None:
        target_item = self._find_tree_item_for_dataset_id(dataset_id)
        if target_item is not None:
            self.tree.setCurrentItem(target_item)

    def _find_tree_item_for_dataset_id(self, dataset_id: str) -> QTreeWidgetItem | None:
        if not dataset_id:
            return None
        for group_index in range(self.tree.topLevelItemCount()):
            group_item = self.tree.topLevelItem(group_index)
            for child_index in range(group_item.childCount()):
                child_item = group_item.child(child_index)
                if self._dataset_id_from_tree_item(child_item) == dataset_id:
                    return child_item
        return None

    def _set_review_dataset_table_selection(self, dataset_id: str) -> None:
        if not hasattr(self, "review_dataset_table"):
            return
        selection_model = self.review_dataset_table.selectionModel()
        if selection_model is None:
            return
        self.review_dataset_table.clearSelection()
        if not dataset_id:
            return
        for row_index, row_dataset_id in enumerate(self._dataset_browser_row_ids):
            if row_dataset_id != dataset_id:
                continue
            self.review_dataset_table.selectRow(row_index)
            item = self.review_dataset_table.item(row_index, 0)
            if item is not None:
                self.review_dataset_table.scrollToItem(item)
            return

    def select_first_review_dataset(self) -> None:
        if not self._dataset_browser_row_ids:
            return
        self._select_review_dataset_row(0)

    def select_previous_review_dataset(self) -> None:
        row = self._current_review_dataset_row()
        if row > 0:
            self._select_review_dataset_row(row - 1)

    def select_next_review_dataset(self) -> None:
        row = self._current_review_dataset_row()
        if 0 <= row < len(self._dataset_browser_row_ids) - 1:
            self._select_review_dataset_row(row + 1)

    def select_last_review_dataset(self) -> None:
        if not self._dataset_browser_row_ids:
            return
        self._select_review_dataset_row(len(self._dataset_browser_row_ids) - 1)

    def _select_review_dataset_row(self, row: int) -> None:
        if row < 0 or row >= len(self._dataset_browser_row_ids):
            return
        self.review_dataset_table.clearSelection()
        self.review_dataset_table.selectRow(row)
        item = self.review_dataset_table.item(row, 0)
        if item is not None:
            self.review_dataset_table.scrollToItem(item)

    def _update_dataset_navigation_buttons(self) -> None:
        row = self._current_review_dataset_row()
        has_rows = bool(self._dataset_browser_row_ids)
        self.dataset_nav_first_button.setEnabled(has_rows and row != 0)
        self.dataset_nav_back_button.setEnabled(has_rows and row > 0)
        self.dataset_nav_next_button.setEnabled(has_rows and 0 <= row < len(self._dataset_browser_row_ids) - 1)
        self.dataset_nav_last_button.setEnabled(has_rows and row != len(self._dataset_browser_row_ids) - 1)

    def on_tree_item_changed(self, item: QTreeWidgetItem, _column: int) -> None:
        if self._populating_tree or self.repository is None:
            return
        if item.parent() is None:
            state = item.checkState(0)
            if state == Qt.PartiallyChecked:
                return
            dataset_ids = self._dataset_ids_from_group_item(item)
            if state == Qt.Checked:
                self._checked_dataset_id_set.update(dataset_ids)
            else:
                self._checked_dataset_id_set.difference_update(dataset_ids)
            self._update_manage_data_scope_summary()
            return
        dataset_id = item.data(0, Qt.UserRole)
        if not dataset_id:
            return
        if item.checkState(0) == Qt.Checked:
            self._checked_dataset_id_set.add(str(dataset_id))
        else:
            self._checked_dataset_id_set.discard(str(dataset_id))
        self._update_manage_data_scope_summary()

    def on_tree_order_changed(self) -> None:
        if self.repository is None or self._populating_tree:
            return
        group_order: list[str] = []
        dataset_order: list[tuple[str, str, int]] = []
        for group_index in range(self.tree.topLevelItemCount()):
            group_item = self.tree.topLevelItem(group_index)
            group_id = str(group_item.data(0, Qt.UserRole) or "ungrouped")
            group_order.append(group_id)
            for child_index in range(group_item.childCount()):
                child = group_item.child(child_index)
                dataset_id = str(child.data(0, Qt.UserRole))
                dataset_order.append((dataset_id, group_id, child_index))
        self.repository.update_ordering(group_order, dataset_order)
        self.refresh_all_views()

    def selected_dataset_id(self) -> str:
        item = self.tree.currentItem()
        return self._dataset_id_from_tree_item(item)

    def selected_dataset(self):
        dataset_id = self.selected_dataset_id()
        if not dataset_id or self.repository is None:
            return None
        return self.repository.get_dataset(dataset_id)

    def selected_group_id(self) -> str:
        item = self.tree.currentItem()
        if item is None:
            return ""
        dataset_id = item.data(0, Qt.UserRole)
        if item.parent() is None:
            return str(dataset_id or "")
        parent = item.parent()
        return str(parent.data(0, Qt.UserRole) or "")

    def selected_dataset_ids_for_action(self) -> list[str]:
        return self._checked_dataset_ids()

    def _dataset_id_from_tree_item(self, item: QTreeWidgetItem | None) -> str:
        if item is None:
            return ""
        data = item.data(0, Qt.UserRole)
        if not data:
            return ""
        dataset = self.repository.get_dataset(str(data)) if self.repository is not None else None
        return dataset.dataset_id if dataset else ""

    def _dataset_ids_from_group_item(self, group_item: QTreeWidgetItem | None) -> list[str]:
        if group_item is None:
            return []
        dataset_ids: list[str] = []
        for index in range(group_item.childCount()):
            dataset_id = self._dataset_id_from_tree_item(group_item.child(index))
            if dataset_id:
                dataset_ids.append(dataset_id)
        return dataset_ids

    def _checked_dataset_ids(self) -> list[str]:
        if self.repository is None:
            return []
        available_ids = {dataset.dataset_id for dataset in self._datasets()}
        self._checked_dataset_id_set.intersection_update(available_ids)
        dataset_ids: list[str] = []
        seen: set[str] = set()
        for group_index in range(self.tree.topLevelItemCount()):
            group_item = self.tree.topLevelItem(group_index)
            for child_index in range(group_item.childCount()):
                child_item = group_item.child(child_index)
                dataset_id = self._dataset_id_from_tree_item(child_item)
                if not dataset_id or dataset_id in seen or dataset_id not in self._checked_dataset_id_set:
                    continue
                seen.add(dataset_id)
                dataset_ids.append(dataset_id)
        return dataset_ids

    def _dataset_ids_for_group_id(self, group_id: str) -> list[str]:
        if self.repository is None:
            return []
        return [dataset.dataset_id for dataset in self._datasets() if dataset.group_id == group_id]

    def _dataset_ids_for_scope(self, scope: str) -> list[str]:
        if scope == "all":
            return [dataset.dataset_id for dataset in self._datasets()]
        if scope == "visible":
            return [dataset.dataset_id for dataset in self._datasets() if dataset.visibility]
        return self._checked_dataset_ids()

    def _review_scope(self) -> str:
        return "checked"

    def _grouping_scope(self) -> str:
        return "checked"

    def _map_scope(self) -> str:
        return str(self.map_scope_combo.currentData() or "visible")

    def _selected_batch_group_id(self) -> str:
        if not hasattr(self, "selection_group_combo"):
            return ""
        return sanitize_group_id(str(self.selection_group_combo.currentData() or ""))

    def _sorted_group_choices(self, groups: list[tuple[str, str]]) -> list[tuple[str, str]]:
        return sorted(groups, key=lambda item: (item[1].casefold(), item[0]))

    def _populate_selection_group_combo(self, preferred_group_id: str | None = None) -> None:
        if not hasattr(self, "selection_group_combo"):
            return
        current_group_id = preferred_group_id or self._selected_batch_group_id() or self.selected_group_id() or "ungrouped"
        has_repository = self.repository is not None
        self.selection_group_combo.blockSignals(True)
        try:
            self.selection_group_combo.clear()
            if not has_repository:
                self.selection_group_combo.setEnabled(False)
            else:
                groups = self._sorted_group_choices(self.repository.list_groups())
                for group_id, group_name in groups:
                    self.selection_group_combo.addItem(group_name, group_id)
                target_group_id = sanitize_group_id(current_group_id or "ungrouped")
                selected_index = self.selection_group_combo.findData(target_group_id)
                if selected_index < 0 and self.selection_group_combo.count() > 0:
                    selected_index = 0
                self.selection_group_combo.setEnabled(self.selection_group_combo.count() > 0)
                if selected_index >= 0:
                    self.selection_group_combo.setCurrentIndex(selected_index)
        finally:
            self.selection_group_combo.blockSignals(False)
        self._update_manage_data_scope_summary()

    def _update_manage_data_scope_summary(self) -> None:
        if not hasattr(self, "selection_scope_status_label"):
            return
        if self.repository is None:
            self.selection_scope_status_label.setText("No project loaded.")
            return
        checked_dataset_ids = set(self._checked_dataset_ids())
        checked_count = len(checked_dataset_ids)
        checked_group_count = len(
            {
                sanitize_group_id(dataset.group_id or "ungrouped")
                for dataset in self._datasets()
                if dataset.dataset_id in checked_dataset_ids
            }
        )
        group_label = "group" if checked_group_count == 1 else "groups"
        selected_group_id = self._selected_batch_group_id() or "ungrouped"
        group_dataset_count = len(self._dataset_ids_for_group_id(selected_group_id))
        self.selection_scope_status_label.setText(
            f"Working set: {checked_count} checked dataset(s), divided between {checked_group_count} {group_label}. "
            f"Dropdown group: {display_group_name(selected_group_id)} ({group_dataset_count} dataset(s) in catalog)."
        )

    def _configure_manage_data_buttons(self, buttons: list[QPushButton]) -> None:
        for button in buttons:
            button.setFixedHeight(MANAGE_ACTION_BUTTON_HEIGHT_PX)
            button.setFixedWidth(MANAGE_ACTION_BUTTON_WIDTH_PX)
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _configure_compact_manage_data_buttons(self, buttons: list[QPushButton]) -> None:
        for button in buttons:
            button.setFixedHeight(30)
            button.setFixedWidth(120)
            button.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)

    def _group_check_state(self, dataset_ids: list[str]):
        if not dataset_ids:
            return Qt.Unchecked
        checked_count = sum(1 for dataset_id in dataset_ids if dataset_id in self._checked_dataset_id_set)
        if checked_count <= 0:
            return Qt.Unchecked
        if checked_count >= len(dataset_ids):
            return Qt.Checked
        return Qt.PartiallyChecked

    def _prompt_group_count(self, dataset_count: int, scope_label: str) -> int:
        default_value = max(2, min(dataset_count, round(dataset_count ** 0.5) + 1))
        value, ok = QInputDialog.getInt(
            self,
            "AI regroup",
            f"Target number of groups for {scope_label}\n(final result should stay within +/-10%)",
            default_value,
            1,
            max(1, dataset_count),
        )
        if not ok:
            return 0
        return int(value)

    def _suggest_regroup_retry_target(self, dataset_count: int, target_group_count: int) -> int:
        dataset_total = max(1, int(dataset_count or 0))
        target = max(1, min(int(target_group_count or 1), dataset_total))
        if target >= dataset_total:
            return target
        return min(dataset_total, max(target + 2, int(math.ceil(target * 1.3))))

    def _regroup_group_count_bounds(self, requested_target_group_count: int, dataset_count: int) -> tuple[int, int]:
        dataset_total = max(1, int(dataset_count or 0))
        target = max(1, min(int(requested_target_group_count or 1), dataset_total))
        lower_bound = max(1, min(dataset_total, int(math.ceil(target * (1.0 - REGROUP_GROUP_COUNT_TOLERANCE_RATIO)))))
        upper_bound = max(lower_bound, min(dataset_total, int(math.floor(target * (1.0 + REGROUP_GROUP_COUNT_TOLERANCE_RATIO)))))
        return lower_bound, upper_bound

    def _group_count_for_assignments(self, assignments: dict[str, str]) -> int:
        return len(
            {
                str(group_name).strip()
                for group_name in assignments.values()
                if str(group_name).strip()
            }
        )

    def _resolve_regroup_group_count_variance(
        self,
        assignments: dict[str, str],
        requested_target_group_count: int,
    ) -> dict[str, object] | None:
        if not assignments or requested_target_group_count <= 0:
            return None
        actual_group_count = self._group_count_for_assignments(assignments)
        lower_bound, upper_bound = self._regroup_group_count_bounds(requested_target_group_count, len(assignments))
        if lower_bound <= actual_group_count <= upper_bound:
            return None

        tolerance_text = f"{lower_bound}-{upper_bound}"
        if actual_group_count > upper_bound:
            message_box = QMessageBox(self)
            message_box.setIcon(QMessageBox.Question)
            message_box.setWindowTitle("Regroup wants more groups")
            message_box.setText(
                f"AI regroup proposed {actual_group_count} groups for a requested target of {requested_target_group_count}."
            )
            message_box.setInformativeText(
                f"The allowed range is {tolerance_text} groups (+/-10%). "
                f"Retry with {upper_bound} groups to stay within that range, or rerun with {actual_group_count} groups if you want to follow the AI split."
            )
            within_button = message_box.addButton(f"Stay Within {tolerance_text}", QMessageBox.AcceptRole)
            higher_button = message_box.addButton(f"Go Higher ({actual_group_count})", QMessageBox.ActionRole)
            cancel_button = message_box.addButton(QMessageBox.Cancel)
            message_box.setDefaultButton(within_button)
            message_box.exec()
            clicked = message_box.clickedButton()
            if clicked is higher_button:
                return {
                    "action": "retry",
                    "target_group_count": actual_group_count,
                    "message": (
                        f"AI regroup proposed {actual_group_count} groups for requested target {requested_target_group_count}. "
                        f"Retrying with the higher AI-suggested target of {actual_group_count} groups."
                    ),
                }
            if clicked is within_button:
                return {
                    "action": "retry",
                    "target_group_count": upper_bound,
                    "message": (
                        f"AI regroup proposed {actual_group_count} groups for requested target {requested_target_group_count}. "
                        f"Retrying within the allowed +/-10% range at {upper_bound} groups."
                    ),
                }
            return {
                "action": "cancel",
                "message": "AI regroup preview cancelled.",
            }

        message_box = QMessageBox(self)
        message_box.setIcon(QMessageBox.Question)
        message_box.setWindowTitle("Regroup produced too few groups")
        message_box.setText(
            f"AI regroup proposed {actual_group_count} groups for a requested target of {requested_target_group_count}."
        )
        message_box.setInformativeText(
            f"The allowed range is {tolerance_text} groups (+/-10%). "
            f"Retry with {lower_bound} groups to stay closer to your requested range, or review the current proposal anyway."
        )
        within_button = message_box.addButton(f"Retry at {lower_bound}", QMessageBox.AcceptRole)
        review_button = message_box.addButton("Review Current Proposal", QMessageBox.ActionRole)
        cancel_button = message_box.addButton(QMessageBox.Cancel)
        message_box.setDefaultButton(within_button)
        message_box.exec()
        clicked = message_box.clickedButton()
        if clicked is within_button:
            return {
                "action": "retry",
                "target_group_count": lower_bound,
                "message": (
                    f"AI regroup proposed {actual_group_count} groups for requested target {requested_target_group_count}. "
                    f"Retrying within the allowed +/-10% range at {lower_bound} groups."
                ),
            }
        if clicked is review_button:
            return {
                "action": "continue",
                "message": (
                    f"Reviewing the current {actual_group_count}-group proposal even though it is outside the allowed +/-10% range."
                ),
            }
        return {
            "action": "cancel",
            "message": "AI regroup preview cancelled.",
        }

    def _datasets_with_source_style(self, dataset_ids: list[str]):
        if self.repository is None:
            return []
        datasets = []
        for dataset_id in dataset_ids:
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None or not dataset.has_source_style:
                continue
            datasets.append(dataset)
        return datasets

    def _confirm_style_generation_for_dataset_ids(self, dataset_ids: list[str]) -> bool:
        flagged_datasets = self._datasets_with_source_style(dataset_ids)
        if not flagged_datasets:
            return True
        preview_lines = [
            f"- {dataset.preferred_name}: {dataset.source_style_summary}"
            for dataset in flagged_datasets[:4]
        ]
        if len(flagged_datasets) > 4:
            preview_lines.append(f"- +{len(flagged_datasets) - 4} more dataset(s)")
        message = (
            f"Possible source styling was detected for {len(flagged_datasets)} selected dataset(s).\n\n"
            "Generating AI styles does not modify the source files, but it will create new GRASP styling for map preview and export.\n\n"
            "Detected examples:\n"
            f"{chr(10).join(preview_lines)}\n\n"
            "Review the existing source styling first if you want that styling to remain the reference. Continue anyway?"
        )
        answer = QMessageBox.question(
            self,
            "Existing source styling detected",
            message,
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        return answer == QMessageBox.Yes

    def _create_regroup_confirmation_dialog(self, assignments: dict[str, str]) -> QDialog | None:
        if self.repository is None or not assignments:
            return None
        grouped: dict[str, list[str]] = {}
        for dataset_id, group_name in assignments.items():
            dataset = self.repository.get_dataset(dataset_id)
            dataset_name = dataset.preferred_name if dataset is not None else dataset_id
            grouped.setdefault(str(group_name).strip() or REGROUP_OTHERS_GROUP_NAME, []).append(dataset_name)
        preview_lines: list[str] = []
        grouped_items = sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0].lower()))
        for group_name, dataset_names in grouped_items:
            preview_lines.append(f"{group_name} ({len(dataset_names)})")
            for dataset_name in sorted(dataset_names, key=str.lower):
                preview_lines.append(f"  - {dataset_name}")
        dialog = QDialog(self)
        dialog.setWindowTitle("Review regroup proposal")
        dialog.setModal(True)
        dialog.setObjectName("regroupReviewDialog")
        dialog.setMinimumWidth(560)
        dialog.resize(680, 520)

        layout = QVBoxLayout(dialog)

        summary_label = QLabel(f"AI Regroup proposed {len(grouped)} group(s) for {len(assignments)} dataset(s).")
        summary_label.setObjectName("regroupSummaryLabel")
        summary_label.setWordWrap(True)
        summary_label.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(summary_label)

        preview_label = QLabel("Preview:")
        preview_label.setObjectName("regroupPreviewLabel")
        layout.addWidget(preview_label)

        # Keep the approval dialog usable even when the regroup preview is long.
        preview_box = QPlainTextEdit()
        preview_box.setObjectName("regroupPreviewBox")
        preview_box.setReadOnly(True)
        preview_box.setLineWrapMode(QPlainTextEdit.WidgetWidth)
        preview_box.setMinimumHeight(220)
        preview_box.setPlainText("\n".join(preview_lines))
        layout.addWidget(preview_box, 1)

        question_label = QLabel(
            "Apply these group assignments? You can still drag datasets between groups in the Datasets overview tab afterward."
        )
        question_label.setObjectName("regroupQuestionLabel")
        question_label.setWordWrap(True)
        layout.addWidget(question_label)

        button_box = QDialogButtonBox(QDialogButtonBox.Yes | QDialogButtonBox.No)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        yes_button = button_box.button(QDialogButtonBox.Yes)
        if yes_button is not None:
            yes_button.setDefault(True)
        layout.addWidget(button_box)
        return dialog

    def _confirm_regroup_assignments(self, assignments: dict[str, str]) -> bool:
        dialog = self._create_regroup_confirmation_dialog(assignments)
        if dialog is None:
            return False
        return dialog.exec() == QDialog.Accepted

    def populate_inspector(self, dataset) -> None:
        self._populating_inspector = True
        try:
            if dataset is None:
                self.dataset_name_edit.clear()
                self.dataset_description_edit.clear()
                self.dataset_group_combo.clear()
                self.dataset_group_combo.setEnabled(False)
                self.visibility_checkbox.setChecked(False)
                self.include_export_checkbox.setChecked(False)
                self.source_path_label.setText("-")
                self.geometry_label.setText("-")
                self.feature_count_label.setText("-")
                self.source_style_label.setText("-")
                self.source_style_label.setToolTip("")
                self.ai_title_label.setText("-")
                self.ai_group_label.setText("-")
                self.ai_description_box.setPlainText("")
                return
            self.dataset_name_edit.setText(dataset.display_name_user)
            self.dataset_description_edit.setPlainText(dataset.description_user)
            self._populate_dataset_group_combo(dataset.group_id)
            self.dataset_group_combo.setEnabled(True)
            self.visibility_checkbox.setChecked(dataset.visibility)
            self.include_export_checkbox.setChecked(dataset.include_in_export)
            self.source_path_label.setText(dataset.source_path)
            self.geometry_label.setText(dataset.geometry_type)
            self.feature_count_label.setText(str(dataset.feature_count))
            self.source_style_label.setText(dataset.source_style_summary or "-")
            self.source_style_label.setToolTip(describe_source_style_evidence(dataset.source_style_items))
            self.ai_title_label.setText(dataset.display_name_ai or "-")
            self.ai_group_label.setText(dataset.suggested_group or "-")
            self.ai_description_box.setPlainText(dataset.description_ai or "")
        finally:
            self._populating_inspector = False

    def _populate_dataset_group_combo(self, current_group_id: str) -> None:
        self.dataset_group_combo.clear()
        if self.repository is None:
            return
        groups = list(self.repository.list_groups())
        normalized_group_id = sanitize_group_id(current_group_id or "ungrouped")
        if not any(group_id == normalized_group_id for group_id, _group_name in groups):
            groups.append((normalized_group_id, display_group_name(normalized_group_id)))
        groups = self._sorted_group_choices(groups)
        for group_id, group_name in groups:
            self.dataset_group_combo.addItem(group_name, group_id)
        selected_index = self.dataset_group_combo.findData(normalized_group_id)
        self.dataset_group_combo.setCurrentIndex(max(selected_index, 0))

    def _selected_dataset_group_id_from_inspector(self) -> str:
        return sanitize_group_id(str(self.dataset_group_combo.currentData() or "ungrouped"))

    def export_gpkg(self) -> None:
        if self.export_service is None or self.current_workspace is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        default_path = self.current_workspace.exports_dir / DEFAULT_EXPORT_GPKG_NAME
        path, _ = QFileDialog.getSaveFileName(self, "Export GeoPackage", str(default_path), "GeoPackage (*.gpkg)")
        if not path:
            return
        self._start_worker(
            self.export_service.export_gpkg,
            path,
            success_message="GeoPackage export completed.",
            start_message="Exporting GeoPackage...",
            activity_name="Export GeoPackage",
        )

    def _style_dataset_ids(self, dataset_ids: list[str], *, status_callback=None, progress_callback=None) -> int:
        if self.repository is None:
            return 0
        total = len(dataset_ids)
        groups = dict(self.repository.list_groups())
        if progress_callback:
            progress_callback(0)
        processed = 0
        skipped_missing = 0
        for index, dataset_id in enumerate(dataset_ids, start=1):
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None:
                skipped_missing += 1
                if status_callback:
                    status_callback(f"Skipping missing dataset {index}/{total}: {dataset_id}")
                continue
            if status_callback:
                status_callback(f"Styling {index}/{total}: {dataset.preferred_name}")
            understanding = DatasetUnderstanding()
            if self.intelligence_service is not None:
                try:
                    understanding = self.intelligence_service.classify(dataset)
                except Exception:
                    understanding = DatasetUnderstanding()
            style_dataset = replace(
                dataset,
                display_name_ai=understanding.suggested_title or dataset.display_name_ai,
                description_ai=understanding.suggested_description or dataset.description_ai,
                suggested_group=understanding.suggested_group or dataset.suggested_group,
                ai_confidence=float(understanding.confidence or dataset.ai_confidence or 0.0),
            )
            style = self.style_service.style_for_dataset(
                style_dataset,
                group_name=groups.get(style_dataset.group_id, style_dataset.group_id),
            )
            self.repository.upsert_style(dataset_id, style)
            processed += 1
            if progress_callback:
                progress_callback(int((processed / max(total, 1)) * 100))
        if progress_callback and total:
            progress_callback(100)
        if status_callback:
            summary = f"Styled {processed}/{total} dataset(s)."
            if skipped_missing:
                summary += f" Skipped {skipped_missing} missing dataset(s)."
            status_callback(summary)
        return processed

    def refresh_all_views(self) -> None:
        self.refresh_import_table()
        self.refresh_tree()
        self._populate_selection_group_combo()
        self.refresh_dataset_browser_table()
        self.refresh_map()

    def refresh_dataset_browser_table(self) -> None:
        if not hasattr(self, "review_dataset_table"):
            return
        selected_id = self.selected_dataset_id()
        filter_text = self.review_dataset_filter_edit.text().strip().lower() if hasattr(self, "review_dataset_filter_edit") else ""
        group_lookup = dict(self.repository.list_groups()) if self.repository is not None else {}
        datasets = [
            dataset
            for dataset in self._datasets()
            if self._dataset_matches_browser_filter(dataset, filter_text, group_lookup)
        ]
        self._dataset_browser_row_ids = [dataset.dataset_id for dataset in datasets]
        self.review_dataset_table.setSortingEnabled(False)
        self.review_dataset_table.clearContents()
        self.review_dataset_table.setRowCount(len(datasets))
        for row_index, dataset in enumerate(datasets):
            group_name = group_lookup.get(dataset.group_id, display_group_name(dataset.group_id))
            dataset_item = SortableTableWidgetItem(dataset.preferred_name, dataset.preferred_name.lower())
            dataset_item.setData(Qt.UserRole, dataset.dataset_id)
            self.review_dataset_table.setItem(row_index, 0, dataset_item)
            self.review_dataset_table.setItem(
                row_index,
                1,
                SortableTableWidgetItem(group_name, group_name.lower()),
            )
            self.review_dataset_table.setItem(
                row_index,
                2,
                SortableTableWidgetItem(dataset.source_format, dataset.source_format.lower()),
            )
        self._syncing_dataset_selection = True
        try:
            self._set_review_dataset_table_selection(selected_id)
        finally:
            self._syncing_dataset_selection = False
        self._update_dataset_navigation_buttons()

    def _dataset_matches_browser_filter(self, dataset, filter_text: str, group_lookup: dict[str, str]) -> bool:
        if not filter_text:
            return True
        group_name = group_lookup.get(dataset.group_id, display_group_name(dataset.group_id))
        haystack = " ".join(
            [
                dataset.preferred_name,
                dataset.source_format,
                dataset.geometry_type,
                dataset.source_path,
                group_name,
                dataset.suggested_group,
            ]
        ).lower()
        return filter_text in haystack

    def refresh_import_table(self) -> None:
        datasets = self._datasets()
        header = self.import_table.horizontalHeader()
        sort_section = header.sortIndicatorSection()
        sort_order = header.sortIndicatorOrder()
        self.import_table.setSortingEnabled(False)
        self.import_table.clearContents()
        self.import_table.setRowCount(len(datasets))
        for row_index, dataset in enumerate(datasets):
            self.import_table.setItem(
                row_index,
                0,
                SortableTableWidgetItem(dataset.preferred_name, dataset.preferred_name.lower()),
            )
            self.import_table.setItem(
                row_index,
                1,
                SortableTableWidgetItem(dataset.source_format, dataset.source_format.lower()),
            )
            self.import_table.setItem(
                row_index,
                2,
                SortableTableWidgetItem(dataset.geometry_type, dataset.geometry_type.lower()),
            )
            self.import_table.setItem(
                row_index,
                3,
                SortableTableWidgetItem(str(dataset.feature_count), int(dataset.feature_count or 0)),
            )
            self.import_table.setItem(
                row_index,
                4,
                SortableTableWidgetItem(
                    "Possible styling" if dataset.has_source_style else "-",
                    1 if dataset.has_source_style else 0,
                ),
            )
            self.import_table.setItem(
                row_index,
                5,
                SortableTableWidgetItem(dataset.source_path, dataset.source_path.lower()),
            )
        self.import_table.setSortingEnabled(True)
        if sort_section >= 0:
            self.import_table.sortItems(sort_section, sort_order)
        if self.repository is None:
            self.import_summary.setText("No folder loaded.")
        else:
            summary = self.repository.summary()
            source_style_count = sum(1 for dataset in datasets if dataset.has_source_style)
            self.import_summary.setText(
                f"Datasets: {summary['dataset_count']} | Groups: {summary['group_count']} | "
                f"Source candidates: {summary['source_count']} | Styles: {summary['style_count']} | "
                f"Possible source styling: {source_style_count}"
            )

    def refresh_tree(self) -> None:
        selected_id = self.selected_dataset_id()
        self._populating_tree = True
        try:
            self.tree.clear()
            if self.repository is None:
                return
            datasets = self.repository.list_datasets()
            self._checked_dataset_id_set.intersection_update({dataset.dataset_id for dataset in datasets})
            group_lookup = self.repository.list_groups()
            datasets_by_group: dict[str, list] = {}
            for dataset in datasets:
                datasets_by_group.setdefault(dataset.group_id or "ungrouped", []).append(dataset)
            for group_id, group_name in group_lookup:
                group_item = QTreeWidgetItem([group_name])
                group_item.setData(0, Qt.UserRole, group_id)
                group_item.setFlags(
                    (group_item.flags() | Qt.ItemIsDropEnabled | Qt.ItemIsUserCheckable | Qt.ItemIsAutoTristate | Qt.ItemIsSelectable)
                    & ~Qt.ItemIsDragEnabled
                )
                self.tree.addTopLevelItem(group_item)
                group_dataset_ids: list[str] = []
                for dataset in sorted(datasets_by_group.get(group_id, []), key=lambda item: item.sort_order):
                    child = QTreeWidgetItem([dataset.preferred_name])
                    child.setData(0, Qt.UserRole, dataset.dataset_id)
                    child.setFlags(child.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsDragEnabled | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    child.setCheckState(0, Qt.Checked if dataset.dataset_id in self._checked_dataset_id_set else Qt.Unchecked)
                    child.setToolTip(0, "Checked = selected for batch actions in Manage data")
                    group_item.addChild(child)
                    group_dataset_ids.append(dataset.dataset_id)
                    if dataset.dataset_id == selected_id:
                        self.tree.setCurrentItem(child)
                group_item.setCheckState(0, self._group_check_state(group_dataset_ids))
                group_item.setToolTip(0, "Checked group = all datasets in the group are selected for batch actions")
                group_item.setExpanded(True)
        finally:
            self._populating_tree = False
        self._update_manage_data_scope_summary()

    def refresh_map(self) -> None:
        datasets = self._datasets()
        style_count = self.repository.summary()["style_count"] if self.repository is not None else 0
        map_scope = self._map_scope()
        map_dataset_ids = self._dataset_ids_for_scope(map_scope) if self.repository is not None else []
        checked_count = len(self._checked_dataset_ids()) if self.repository is not None else 0
        self._map_refresh_pending = True
        if self.repository is None:
            self.map_summary.setText("No project loaded.")
            return
        if self.map_bridge is not None:
            self.map_bridge.set_scope(map_scope)
        map_scope_label = self.map_scope_combo.currentText().strip() or "Visible on map"
        if self._review_job_running:
            self.map_summary.setText(
                f"Map layers in current scope ({map_scope_label}): {len(map_dataset_ids)} of {len(datasets)} | "
                f"Checked working set: {checked_count} | Styled datasets in catalog: {style_count}. "
                "Map loading is paused while dataset processing is running."
            )
            return
        if not self._is_map_tab_active():
            self.map_summary.setText(
                f"Map layers in current scope ({map_scope_label}): {len(map_dataset_ids)} of {len(datasets)} | "
                f"Checked working set: {checked_count} | Styled datasets in catalog: {style_count}. "
                "Open the Map tab to load the map in browse mode."
            )
            return
        self._ensure_map_ready()
        if WEBENGINE_AVAILABLE and not self._map_page_ready:
            self.map_summary.setText(
                f"Map layers in current scope ({map_scope_label}): {len(map_dataset_ids)} of {len(datasets)} | "
                f"Checked working set: {checked_count} | Styled datasets in catalog: {style_count}. "
                "Preparing the embedded map renderer..."
            )
            return
        if self.map_bridge is not None:
            self.map_bridge.set_scope(map_scope)
        scope_note = (
            "using datasets marked Visible on map."
            if map_scope == "visible"
            else "showing all datasets."
        )
        self.map_summary.setText(
            f"Map layers in current scope ({map_scope_label}): {len(map_dataset_ids)} of {len(datasets)} | "
            f"Checked working set: {checked_count} | Styled datasets in catalog: {style_count}. "
            f"The map scope is independent from the checked working set. Browse mode loads one layer at a time by default, {scope_note}"
        )
        if self.map_bridge is not None:
            self.map_bridge.publish_state()
        self._map_refresh_pending = False

    def append_activity_log(self, message: str, activity: str | None = None) -> None:
        lines = [line.strip() for line in str(message).splitlines() if line.strip()]
        if not lines:
            return
        activity_name = self._resolve_log_activity(activity)
        entries = [self._format_log_entry(line, activity_name) for line in lines]
        for entry in entries:
            self.log_text.appendPlainText(entry)
        self._write_activity_log_entries(entries)

    def append_import_log(self, message: str) -> None:
        self.append_activity_log(message)

    def use_ai_for_selected_dataset(self) -> None:
        if self.repository is None:
            return
        dataset = self.selected_dataset()
        if dataset is None:
            return
        ai_name = (dataset.display_name_ai or "").strip()
        ai_description = (dataset.description_ai or "").strip()
        if not ai_name and not ai_description:
            QMessageBox.information(self, "No AI text", "This dataset does not have AI title or description yet.")
            return
        self.repository.save_dataset_user_fields(
            dataset.dataset_id,
            display_name_user=ai_name or dataset.display_name_user,
            description_user=ai_description or dataset.description_user,
            visibility=self.visibility_checkbox.isChecked(),
            include_in_export=self.include_export_checkbox.isChecked(),
        )
        self.append_activity_log(
            f"Transferred AI title/description to Name/Description for {dataset.preferred_name}.",
            activity="Transfer AI Text",
        )
        self.refresh_all_views()

    def _set_workspace(self, folder: str) -> None:
        workspace = ensure_workspace(folder)
        self.current_workspace = workspace
        self.repository = CatalogRepository(workspace.db_path)
        self.ingest_service = IngestService(workspace)
        self.export_service = ExportService(workspace, self.repository)
        self.map_bridge = None
        self.map_channel = None
        self._map_initialized = False
        self._map_page_ready = False
        self._map_refresh_pending = True
        self._load_activity_log()
        self._update_folder_actions()

    def _rebuild_ai_services(self) -> None:
        classifier = OpenAIClassificationProvider(
            api_key=self.current_settings.openai_api_key or None,
            model=self.current_settings.openai_model or DEFAULT_OPENAI_MODEL,
            endpoint=self.current_settings.openai_endpoint,
            managed_data_language=self.current_settings.managed_data_language,
            fallback=HeuristicClassificationProvider(),
            timeout_s=self.current_settings.openai_timeout_s,
            max_consecutive_failures=self.current_settings.openai_max_consecutive_failures,
            include_source_name=self.current_settings.classification_include_source_name,
            include_layer_name=self.current_settings.classification_include_layer_name,
            include_column_names=self.current_settings.classification_include_column_names,
            include_sample_values=self.current_settings.classification_include_sample_values,
            include_geometry_type=self.current_settings.classification_include_geometry_type,
            include_feature_count=self.current_settings.classification_include_feature_count,
            include_bbox=self.current_settings.classification_include_bbox,
        )
        self.intelligence_service = IntelligenceService(classifier=classifier)
        self.search_service = SearchService(
            provider=DuckDuckGoSearchProvider(
                timeout_s=self.current_settings.search_timeout_s,
                max_consecutive_failures=self.current_settings.search_max_consecutive_failures,
                target_candidates=self.current_settings.search_target_candidates,
            ),
            ranker=classifier,
        )

    def _update_model_label(self) -> None:
        model = self.current_settings.openai_model or DEFAULT_OPENAI_MODEL
        data_language = display_managed_data_language(self.current_settings.managed_data_language).lower()
        context_parts: list[str] = []
        if self.current_settings.classification_include_source_name:
            context_parts.append("file")
        if self.current_settings.classification_include_layer_name:
            context_parts.append("layer")
        if self.current_settings.classification_include_column_names:
            context_parts.append("columns")
        if self.current_settings.classification_include_sample_values:
            context_parts.append("samples")
        if self.current_settings.classification_include_geometry_type:
            context_parts.append("geometry")
        if self.current_settings.classification_include_feature_count:
            context_parts.append("feature count")
        if self.current_settings.classification_include_bbox:
            context_parts.append("bbox")
        context_summary = ", ".join(context_parts) if context_parts else "none"
        self.settings_model_label.setText(
            f"Active AI model: {model} (default in code: {DEFAULT_OPENAI_MODEL}) | "
            f"Data language: {data_language} | "
            f"Manual AI context: {context_summary} | "
            f"Live search timeout: {self.current_settings.search_timeout_s:g}s | "
            f"Search failover: {self.current_settings.search_max_consecutive_failures}"
        )

    def _apply_settings_to_form(self, settings: AppSettings) -> None:
        self.settings_model_combo.setCurrentText(settings.openai_model)
        language_index = self.settings_data_language_combo.findData(normalize_managed_data_language(settings.managed_data_language))
        self.settings_data_language_combo.setCurrentIndex(max(language_index, 0))
        self.settings_api_key_edit.setText(settings.openai_api_key)
        self.settings_endpoint_edit.setText(settings.openai_endpoint)
        self.settings_timeout_edit.setText(str(settings.openai_timeout_s))
        self.settings_failures_edit.setText(str(settings.openai_max_consecutive_failures))
        self.settings_context_source_name_checkbox.setChecked(settings.classification_include_source_name)
        self.settings_context_layer_name_checkbox.setChecked(settings.classification_include_layer_name)
        self.settings_context_column_names_checkbox.setChecked(settings.classification_include_column_names)
        self.settings_context_sample_values_checkbox.setChecked(settings.classification_include_sample_values)
        self.settings_context_geometry_checkbox.setChecked(settings.classification_include_geometry_type)
        self.settings_context_feature_count_checkbox.setChecked(settings.classification_include_feature_count)
        self.settings_context_bbox_checkbox.setChecked(settings.classification_include_bbox)
        self.settings_search_timeout_edit.setText(str(settings.search_timeout_s))
        self.settings_search_failures_edit.setText(str(settings.search_max_consecutive_failures))
        self.settings_search_candidates_edit.setText(str(settings.search_target_candidates))
        self._update_model_label()

    def _update_folder_actions(self) -> None:
        folder = self.folder_edit.text().strip()
        has_existing = False
        if folder and Path(folder).exists():
            try:
                has_existing = catalog_exists(folder)
            except Exception:
                has_existing = False
        if hasattr(self, "load_existing_button"):
            self.load_existing_button.setEnabled(has_existing)
        if hasattr(self, "reset_data_button"):
            self.reset_data_button.setEnabled(has_existing)
        if hasattr(self, "load_existing_action"):
            self.load_existing_action.setEnabled(has_existing)
        if hasattr(self, "reset_data_action"):
            self.reset_data_action.setEnabled(has_existing)

    def _remember_last_folder(self, folder: str) -> None:
        normalized = str(Path(folder).expanduser())
        if self.current_settings.last_folder == normalized:
            return
        self.current_settings.last_folder = normalized
        self.settings_store.save(self.current_settings)

    def _build_map_placeholder(self, message: str | None = None) -> QPlainTextEdit:
        placeholder = QPlainTextEdit()
        placeholder.setReadOnly(True)
        if message is None:
            if WEBENGINE_AVAILABLE:
                message = "Open the Map tab to initialize the embedded map preview."
            else:
                message = WEBENGINE_UNAVAILABLE_MESSAGE
        placeholder.setPlainText(message)
        return placeholder

    def _replace_map_view(self, new_view: QWidget) -> None:
        old_view = getattr(self, "map_view", None)
        self.map_view = new_view
        if hasattr(self, "map_view_layout"):
            self.map_view_layout.addWidget(new_view, 1)
            if old_view is not None:
                self.map_view_layout.removeWidget(old_view)
        if old_view is not None and hasattr(old_view, "deleteLater"):
            old_view.deleteLater()

    def _ensure_webengine_map_view(self) -> bool:
        if not WEBENGINE_AVAILABLE:
            return False
        if hasattr(self.map_view, "page"):
            return True
        try:
            map_view = QWebEngineView()
            if LoggingWebEnginePage is not None and hasattr(map_view, "setPage"):
                map_view.setPage(
                    LoggingWebEnginePage(
                        lambda message: self.append_activity_log(message, activity="Map"),
                        map_view,
                    )
                )
            if hasattr(map_view, "loadFinished"):
                map_view.loadFinished.connect(self._on_map_view_loaded)
            if hasattr(map_view, "renderProcessTerminated"):
                map_view.renderProcessTerminated.connect(self._on_map_render_process_terminated)
        except Exception as exc:
            self.append_activity_log(
                f"Qt WebEngine could not be initialized ({exc}). The embedded map preview is unavailable.",
                activity="Map",
            )
            self.map_summary.setText("Qt WebEngine could not be initialized in this environment.")
            self._replace_map_view(
                self._build_map_placeholder(
                    "Qt WebEngine could not be initialized. The embedded map preview is unavailable in this environment."
                )
            )
            return False
        self._replace_map_view(map_view)
        return True

    def _setup_map_bridge(self) -> None:
        if not WEBENGINE_AVAILABLE or self.current_workspace is None or self.repository is None:
            return
        if not self._ensure_webengine_map_view():
            return
        self._map_page_ready = False
        self.map_bridge = MapBridge(self.current_workspace, self.repository)
        page = self.map_view.page()
        profile = page.profile() if hasattr(page, "profile") else None
        if profile is not None:
            if hasattr(profile, "setHttpUserAgent"):
                profile.setHttpUserAgent(MAP_HTTP_USER_AGENT)
            web_cache_dir = self.current_workspace.workspace_path / "web_cache"
            web_profile_dir = self.current_workspace.workspace_path / "web_profile"
            web_cache_dir.mkdir(parents=True, exist_ok=True)
            web_profile_dir.mkdir(parents=True, exist_ok=True)
            if hasattr(profile, "setCachePath"):
                profile.setCachePath(str(web_cache_dir))
            if hasattr(profile, "setPersistentStoragePath"):
                profile.setPersistentStoragePath(str(web_profile_dir))
        if QWebEngineSettings is not None:
            settings = page.settings()
            for attribute_name, enabled in (
                ("LocalContentCanAccessRemoteUrls", True),
                ("LocalContentCanAccessFileUrls", True),
                ("WebGLEnabled", False),
                ("Accelerated2dCanvasEnabled", False),
            ):
                attribute = getattr(QWebEngineSettings, attribute_name, None)
                if attribute is not None:
                    settings.setAttribute(attribute, enabled)
        self.map_channel = QWebChannel(page)
        self.map_channel.registerObject("mapBridge", self.map_bridge)
        page.setWebChannel(self.map_channel)
        html_path = Path(__file__).resolve().parent / "assets" / "leaflet_map.html"
        self.map_view.load(QUrl.fromLocalFile(str(html_path)))

    def _ensure_map_ready(self) -> None:
        if self._map_initialized:
            return
        if WEBENGINE_AVAILABLE:
            self._setup_map_bridge()
        self._map_initialized = True

    def _is_map_tab_active(self) -> bool:
        return self.tabs.currentWidget() is self.map_tab

    def on_tab_changed(self, _index: int) -> None:
        if self.tabs.currentWidget() is self.review_datasets_tab:
            self._sync_review_dataset_splitter_sizes()
        if self.tabs.currentWidget() is self.info_sources_tab:
            self._sync_info_sources_splitter_sizes()
        if not self._is_map_tab_active():
            return
        self.refresh_map()
        if self._map_initialized and self._map_page_ready and WEBENGINE_AVAILABLE and hasattr(self.map_view, "page"):
            try:
                self.map_view.page().runJavaScript("window.dispatchEvent(new Event('resize'));")
            except Exception:
                pass

    def resizeEvent(self, event) -> None:  # type: ignore[override]
        super().resizeEvent(event)
        if self.tabs.currentWidget() is self.review_datasets_tab:
            self._sync_review_dataset_splitter_sizes()
        if self.tabs.currentWidget() is self.info_sources_tab:
            self._sync_info_sources_splitter_sizes()

    def _sync_review_dataset_splitter_sizes(self) -> None:
        if self._updating_review_dataset_splitter or not hasattr(self, "review_dataset_splitter"):
            return
        if self.review_dataset_splitter.count() < 2:
            return
        available_width = self.review_dataset_splitter.width() or self.review_datasets_tab.width()
        if available_width <= 0:
            return
        if available_width < 900:
            browser_ratio = 0.44
        elif available_width < 1400:
            browser_ratio = 0.38
        else:
            browser_ratio = 0.34
        browser_min_width = 220
        details_min_width = 320
        browser_width = int(available_width * browser_ratio)
        if available_width >= browser_min_width + details_min_width:
            browser_width = max(browser_min_width, min(browser_width, available_width - details_min_width))
        else:
            browser_width = max(160, browser_width)
        details_width = max(0, available_width - browser_width)
        self._updating_review_dataset_splitter = True
        try:
            self.review_dataset_splitter.setSizes([browser_width, details_width])
        finally:
            self._updating_review_dataset_splitter = False

    def _sync_info_sources_splitter_sizes(self) -> None:
        if self._info_sources_splitter_initialized or not hasattr(self, "info_sources_splitter"):
            return
        if self.info_sources_splitter.count() < 2:
            return
        available_width = self.info_sources_splitter.width() or self.info_sources_tab.width()
        if available_width <= 0:
            return
        half_width = max(0, available_width // 2)
        self.info_sources_splitter.setSizes([half_width, max(0, available_width - half_width)])
        self._info_sources_splitter_initialized = True

    def _on_map_view_loaded(self, ok: bool) -> None:
        self._map_page_ready = bool(ok)
        if not ok:
            self.append_activity_log("Embedded map page failed to load.", activity="Map")
            self.map_summary.setText("The embedded map page failed to load. Try Refresh Map or restart the app.")
            return
        self.append_activity_log("Embedded map page loaded.", activity="Map")
        if self._map_refresh_pending and self._is_map_tab_active() and not self._review_job_running:
            self.refresh_map()

    def _on_map_render_process_terminated(self, termination_status, exit_code: int) -> None:
        status_name = getattr(termination_status, "name", str(termination_status))
        self.append_activity_log(
            f"Embedded map render process terminated ({status_name}, exit code {exit_code}).",
            activity="Map",
        )
        self.map_summary.setText(
            "The embedded map renderer stopped unexpectedly. Try Refresh Map. "
            f"Renderer status: {status_name}, exit code {exit_code}."
        )

    def _datasets(self):
        if self.repository is None:
            return []
        return self.repository.list_datasets()

    def _start_worker_with_refresh(
        self,
        fn,
        dataset_ids: list[str],
        success_message: str,
        start_message: str = "Running background job...",
        activity_name: str | None = None,
    ) -> None:
        self._start_worker(
            fn,
            dataset_ids,
            success_message=success_message,
            start_message=start_message,
            activity_name=activity_name,
            refresh_after=True,
            track_review_job=True,
        )

    def _start_regroup_preview_job(
        self,
        dataset_ids: list[str],
        target_group_count: int,
        *,
        scope_label: str,
    ) -> None:
        worker = FunctionWorker(self._prepare_regroup_assignments, dataset_ids, target_group_count)
        log_activity = self._resolve_log_activity("AI Regroup")
        start_message = f"Regrouping {scope_label}..."
        progress_token = self._begin_background_activity(start_message, activity=log_activity)
        self._active_workers[progress_token] = worker
        self._review_job_running = True
        self.review_progress.setValue(0)
        self.review_job_status.setText("Starting background job...")
        worker.signals.status.connect(self.statusBar().showMessage)
        worker.signals.status.connect(lambda message, activity=log_activity: self.append_activity_log(message, activity=activity))
        worker.signals.status.connect(lambda message, token=progress_token: self._update_background_activity_status(token, message))
        worker.signals.status.connect(self.review_job_status.setText)
        worker.signals.progress.connect(lambda value, token=progress_token: self._update_background_activity_progress(token, value))
        worker.signals.progress.connect(self.review_progress.setValue)
        worker.signals.result.connect(
            lambda proposal, token=progress_token, scope_label=scope_label: self._schedule_regroup_preview(
                token,
                proposal,
                scope_label,
            )
        )
        worker.signals.error.connect(lambda message, token=progress_token: self.on_background_error(message, token))
        worker.signals.finished.connect(lambda token=progress_token: self._release_worker(token))
        self.thread_pool.start(worker)

    def _schedule_regroup_preview(self, token: int, proposal, scope_label: str) -> None:
        QTimer.singleShot(0, lambda token=token, proposal=proposal, scope_label=scope_label: self._complete_regroup_preview(token, proposal, scope_label))

    def _complete_regroup_preview(self, token: int, proposal, scope_label: str = "selected datasets") -> None:
        try:
            assignments = dict((proposal or {}).get("assignments") or {})
            dataset_ids = list((proposal or {}).get("dataset_ids") or assignments.keys())
            requested_target_group_count = int((proposal or {}).get("target_group_count") or 0)
            if not assignments:
                self._update_background_activity_progress(token, 100)
                self.review_progress.setValue(100)
                self._finish_background_activity(token, "AI regroup produced no assignments.")
                self.statusBar().showMessage("AI regroup produced no assignments.", 5000)
                self._on_review_job_finished("AI regroup produced no assignments.")
                return
            regroup_decision = self._resolve_regroup_group_count_variance(assignments, requested_target_group_count)
            if regroup_decision is not None:
                decision_message = str(regroup_decision.get("message") or "").strip()
                if decision_message:
                    self.append_activity_log(decision_message, activity="AI Regroup")
                    self.statusBar().showMessage(decision_message, 5000)
                action = str(regroup_decision.get("action") or "").strip().lower()
                if action == "retry":
                    next_target_group_count = max(1, int(regroup_decision.get("target_group_count") or requested_target_group_count or 1))
                    self._update_background_activity_progress(token, 100)
                    self.review_progress.setValue(100)
                    self._finish_background_activity(token, decision_message or "Retrying AI regroup with a new group target.")
                    self._on_review_job_finished(decision_message or "Retrying AI regroup with a new group target.")
                    self._start_regroup_preview_job(dataset_ids, next_target_group_count, scope_label=scope_label)
                    return
                if action == "cancel":
                    self._update_background_activity_progress(token, 100)
                    self.review_progress.setValue(100)
                    self._finish_background_activity(token, decision_message or "AI regroup preview cancelled.")
                    self._on_review_job_finished(decision_message or "AI regroup preview cancelled.")
                    return
            accepted = self._confirm_regroup_assignments(assignments)
            if not accepted:
                self.append_activity_log("Preview cancelled. Group assignments were not applied.", activity="AI Regroup")
                self._update_background_activity_progress(token, 100)
                self.review_progress.setValue(100)
                self._finish_background_activity(token, "AI regroup preview cancelled.")
                self.statusBar().showMessage("AI regroup preview cancelled.", 5000)
                self._on_review_job_finished("AI regroup preview cancelled.")
                return

            def _status_callback(message: str) -> None:
                self.statusBar().showMessage(message)
                self.append_activity_log(message, activity="AI Regroup")
                self._update_background_activity_status(token, message)
                self.review_job_status.setText(message)

            def _progress_callback(value: int) -> None:
                self._update_background_activity_progress(token, value)
                self.review_progress.setValue(value)

            applied = self._apply_regroup_assignments(
                assignments,
                status_callback=_status_callback,
                progress_callback=_progress_callback,
            )
            self.append_activity_log("Applying results to the catalog and refreshing views.", activity="AI Regroup")
            self.refresh_all_views()
            success_message = f"AI regrouping completed for {applied} dataset(s)."
            self._finish_background_activity(token, success_message)
            self.statusBar().showMessage(success_message, 5000)
            self._on_review_job_finished(success_message)
        except Exception:
            self.on_background_error(traceback.format_exc(), token)

    def _run_review_job_foreground_with_refresh(
        self,
        fn,
        dataset_ids: list[str],
        success_message: str,
        start_message: str = "Running background job...",
        activity_name: str | None = None,
    ) -> None:
        log_activity = self._resolve_log_activity(activity_name or start_message)
        progress_token = self._begin_background_activity(start_message, activity=log_activity)
        self._review_job_running = True
        self.review_progress.setValue(0)
        self.review_job_status.setText("Starting background job...")
        app = QApplication.instance()

        def _pump_events() -> None:
            if app is not None:
                app.processEvents()

        def _status_callback(message: str) -> None:
            self.statusBar().showMessage(message)
            self.append_activity_log(message, activity=log_activity)
            self._update_background_activity_status(progress_token, message)
            self.review_job_status.setText(message)
            _pump_events()

        def _progress_callback(value: int) -> None:
            self._update_background_activity_progress(progress_token, value)
            self.review_progress.setValue(value)
            _pump_events()

        try:
            fn(dataset_ids, status_callback=_status_callback, progress_callback=_progress_callback)
        except Exception:
            self.on_background_error(traceback.format_exc(), progress_token)
            return
        self._refresh_all_views_after_worker(log_activity)
        self._finish_background_activity(progress_token, success_message)
        self.statusBar().showMessage(success_message, 5000)
        self._on_review_job_finished(success_message)

    def _start_worker(
        self,
        fn,
        *args,
        success_message: str,
        start_message: str = "Running background job...",
        activity_name: str | None = None,
        refresh_after: bool = False,
        track_review_job: bool = False,
    ) -> None:
        worker = FunctionWorker(fn, *args)
        log_activity = self._resolve_log_activity(activity_name or start_message)
        progress_token = self._begin_background_activity(start_message, activity=log_activity)
        self._active_workers[progress_token] = worker
        worker.signals.status.connect(self.statusBar().showMessage)
        worker.signals.status.connect(lambda message, activity=log_activity: self.append_activity_log(message, activity=activity))
        worker.signals.status.connect(lambda message, token=progress_token: self._update_background_activity_status(token, message))
        worker.signals.progress.connect(lambda value, token=progress_token: self._update_background_activity_progress(token, value))
        if track_review_job:
            self._review_job_running = True
            self.review_progress.setValue(0)
            self.review_job_status.setText("Starting background job...")
            worker.signals.status.connect(self.review_job_status.setText)
            worker.signals.progress.connect(self.review_progress.setValue)
        worker.signals.error.connect(lambda message, token=progress_token: self.on_background_error(message, token))
        worker.signals.result.connect(
            lambda _value, token=progress_token, activity=log_activity, message=success_message, needs_refresh=refresh_after, review_job=track_review_job:
            self._complete_worker_success(
                token,
                message,
                activity=activity,
                refresh_after=needs_refresh,
                track_review_job=review_job,
            )
        )
        worker.signals.finished.connect(lambda token=progress_token: self._release_worker(token))
        self.thread_pool.start(worker)

    def _heuristic_classify_dataset_ids(self, dataset_ids: list[str], *, status_callback=None, progress_callback=None) -> int:
        return self._classify_dataset_ids_with_service(
            dataset_ids,
            self.heuristic_intelligence_service,
            status_prefix="Fast local classification",
            status_callback=status_callback,
            progress_callback=progress_callback,
            time_budget_s=INITIAL_HEURISTIC_CLASSIFICATION_TIMEOUT_S,
        )

    def _classify_dataset_ids(self, dataset_ids: list[str], *, status_callback=None, progress_callback=None) -> int:
        return self._classify_dataset_ids_with_service(
            dataset_ids,
            self.intelligence_service,
            status_prefix="Finding info with AI",
            status_callback=status_callback,
            progress_callback=progress_callback,
        )

    def _classify_dataset_ids_with_service(
        self,
        dataset_ids: list[str],
        service,
        *,
        status_prefix: str,
        status_callback=None,
        progress_callback=None,
        time_budget_s: float | None = None,
    ) -> int:
        if self.repository is None:
            return 0
        total = len(dataset_ids)
        if progress_callback:
            progress_callback(0)
        started_at = monotonic() if time_budget_s else None
        processed = 0
        skipped_missing = 0
        pending_updates: list[tuple[str, DatasetUnderstanding]] = []
        provider = self._openai_provider_for_service(service)
        last_logged_provider_issue = ""
        if status_callback and time_budget_s:
            status_callback(
                f"{status_prefix} has a {self._format_elapsed_seconds(time_budget_s)} time budget for this pass."
            )
        for index, dataset_id in enumerate(dataset_ids, start=1):
            if time_budget_s is not None and started_at is not None:
                elapsed = monotonic() - started_at
                if elapsed >= time_budget_s:
                    self._flush_understanding_updates(
                        pending_updates,
                        status_callback=status_callback,
                    )
                    remaining = max(0, total - processed)
                    if status_callback:
                        status_callback(
                            f"{status_prefix} time budget reached after {self._format_elapsed_seconds(elapsed)}. "
                            f"Leaving {remaining} dataset(s) unchanged for now."
                        )
                    break
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None:
                skipped_missing += 1
                if status_callback:
                    status_callback(f"Skipping missing dataset {index}/{total}: {dataset_id}")
                continue
            remote_available = provider.remote_availability_status()[0] if provider is not None else False
            if status_callback:
                if provider is not None and remote_available:
                    status_callback(
                        f"{status_prefix} {index}/{total}: {dataset.preferred_name} "
                        f"(waiting for AI response)"
                    )
                elif provider is not None:
                    status_callback(
                        f"{status_prefix} {index}/{total}: {dataset.preferred_name} "
                        f"(using heuristic fallback)"
                    )
                else:
                    status_callback(f"{status_prefix} {index}/{total}: {dataset.preferred_name}")
            understanding = service.classify(dataset)
            provider_issue = self._consume_openai_provider_issue(provider)
            if provider_issue and provider_issue != last_logged_provider_issue:
                if status_callback:
                    status_callback(provider_issue)
                last_logged_provider_issue = provider_issue
            pending_updates.append((dataset_id, understanding))
            processed += 1
            if progress_callback:
                progress_callback(int((processed / max(total, 1)) * 100))
            if len(pending_updates) >= UNDERSTANDING_PERSIST_BATCH_SIZE:
                self._flush_understanding_updates(
                    pending_updates,
                    status_callback=status_callback,
                )
            if provider is not None and remote_available and index < total:
                sleep(REMOTE_AI_REQUEST_COOLDOWN_S)
        self._flush_understanding_updates(
            pending_updates,
            status_callback=status_callback,
        )
        if progress_callback and total:
            progress_callback(100)
        if status_callback:
            if time_budget_s and processed < total:
                status_callback(
                    f"{status_prefix} finished this pass after processing {processed}/{total} dataset(s)."
                )
            else:
                summary = f"Completed {status_prefix.lower()} for {processed}/{total} dataset(s)."
                if skipped_missing:
                    summary += f" Skipped {skipped_missing} missing dataset(s)."
                status_callback(summary)
        return processed

    def _ai_runtime_note(self, dataset_count: int) -> str:
        if dataset_count <= 0:
            return ""
        provider = self._openai_provider()
        if provider is None:
            return f"AI understanding will run sequentially for {dataset_count} dataset(s)."
        remote_available, availability_message = provider.remote_availability_status()
        if not remote_available:
            return (
                f"{availability_message} "
                f"The requested job will use heuristic fallback for {dataset_count} dataset(s)."
            )
        timeout_s = getattr(provider, "timeout_s", None)
        if timeout_s is None:
            return f"AI understanding will run sequentially for {dataset_count} dataset(s)."
        try:
            timeout_value = max(0, int(round(float(timeout_s))))
        except (TypeError, ValueError):
            return f"AI understanding will run sequentially for {dataset_count} dataset(s)."
        worst_case_seconds = timeout_value * dataset_count
        return (
            f"AI understanding will run sequentially for {dataset_count} dataset(s). "
            f"Current per-dataset timeout is {timeout_value}s, so the worst-case upper bound is "
            f"about {self._format_elapsed_seconds(worst_case_seconds)}. "
            f"A short {REMOTE_AI_REQUEST_COOLDOWN_S:.2f}s cooldown is inserted between remote AI requests."
        )

    def _openai_provider(self) -> OpenAIClassificationProvider | None:
        return self._openai_provider_for_service(self.intelligence_service)

    def _openai_provider_for_service(self, service) -> OpenAIClassificationProvider | None:
        if service is None:
            return None
        classifier = getattr(service, "classifier", None)
        if isinstance(classifier, OpenAIClassificationProvider):
            return classifier
        if isinstance(service, OpenAIClassificationProvider):
            return service
        return None

    def _consume_openai_provider_issue(self, provider: OpenAIClassificationProvider | None) -> str:
        if provider is None:
            return ""
        issue = provider.consume_last_error_message()
        if not issue:
            return ""
        return issue

    def _flush_understanding_updates(
        self,
        pending_updates: list[tuple[str, DatasetUnderstanding]],
        *,
        status_callback=None,
    ) -> None:
        if not pending_updates or self.repository is None:
            return
        batch_size = len(pending_updates)
        if status_callback:
            status_callback(f"Persisting {batch_size} understanding update(s) to the catalog.")
        self.repository.upsert_understandings_bulk(
            pending_updates,
            auto_assign_group=False,
        )
        pending_updates.clear()

    def _classify_and_search_dataset_ids(self, dataset_ids: list[str], *, status_callback=None, progress_callback=None) -> int:
        if self.repository is None:
            return 0
        total = len(dataset_ids)
        if progress_callback:
            progress_callback(0)
        processed = 0
        skipped_missing = 0
        for index, dataset_id in enumerate(dataset_ids, start=1):
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None:
                skipped_missing += 1
                if status_callback:
                    status_callback(f"Skipping missing dataset {index}/{total}: {dataset_id}")
                continue
            if status_callback:
                status_callback(f"AI + sources {index}/{total}: {dataset.preferred_name}")
            understanding = self.intelligence_service.classify(dataset)
            sources = self.search_service.find_sources(understanding, dataset)
            enrich_from_sources = getattr(self.intelligence_service, "enrich_from_sources", None)
            if callable(enrich_from_sources):
                understanding = enrich_from_sources(dataset, understanding, sources)
            self.repository.upsert_understanding(dataset_id, understanding)
            self.repository.replace_sources(dataset_id, sources)
            processed += 1
            if progress_callback:
                progress_callback(int((processed / max(total, 1)) * 100))
        if progress_callback and total:
            progress_callback(100)
        if status_callback:
            summary = f"Completed AI + source enrichment for {processed}/{total} dataset(s)."
            if skipped_missing:
                summary += f" Skipped {skipped_missing} missing dataset(s)."
            status_callback(summary)
        return processed

    def _search_dataset_ids(self, dataset_ids: list[str], *, status_callback=None, progress_callback=None) -> int:
        if self.repository is None:
            return 0
        total = len(dataset_ids)
        if progress_callback:
            progress_callback(0)
        processed = 0
        skipped_missing = 0
        for index, dataset_id in enumerate(dataset_ids, start=1):
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None:
                skipped_missing += 1
                if status_callback:
                    status_callback(f"Skipping missing dataset {index}/{total}: {dataset_id}")
                continue
            understanding = self.repository.get_understanding(dataset_id)
            if not understanding.search_queries:
                understanding = self.intelligence_service.classify(dataset)
                self.repository.upsert_understanding(dataset_id, understanding)
            if status_callback:
                status_callback(f"Finding sources {index}/{total}: {dataset.preferred_name}")
            sources = self.search_service.find_sources(understanding, dataset)
            enrich_from_sources = getattr(self.intelligence_service, "enrich_from_sources", None)
            if callable(enrich_from_sources):
                understanding = enrich_from_sources(dataset, understanding, sources)
            self.repository.upsert_understanding(dataset_id, understanding)
            self.repository.replace_sources(dataset_id, sources)
            processed += 1
            if progress_callback:
                progress_callback(int((processed / max(total, 1)) * 100))
        if progress_callback and total:
            progress_callback(100)
        if status_callback:
            summary = f"Completed source lookup for {processed}/{total} dataset(s)."
            if skipped_missing:
                summary += f" Skipped {skipped_missing} missing dataset(s)."
            status_callback(summary)
        return processed

    def _prepare_regroup_assignments(
        self,
        dataset_ids: list[str],
        target_group_count: int,
        *,
        status_callback=None,
        progress_callback=None,
    ) -> int:
        if self.repository is None:
            return 0
        datasets: list = []
        regroup_started_at = monotonic()
        prepared_dataset_ids: set[str] = set()
        timed_out_dataset_ids: list[str] = []
        total = len(dataset_ids)
        if progress_callback:
            progress_callback(0)
        if status_callback:
            status_callback(
                f"Regroup has a total time budget of {self._format_elapsed_seconds(REGROUP_TOTAL_TIMEOUT_S)}. "
                f"Preparing fresh regroup input for {total} dataset(s). "
                "Current group assignments and cached AI grouping hints will be ignored for this run. "
                f"Any remaining dataset(s) after that will go to {REGROUP_OTHERS_GROUP_NAME}."
            )
        for index, dataset_id in enumerate(dataset_ids, start=1):
            elapsed = monotonic() - regroup_started_at
            if elapsed >= REGROUP_TOTAL_TIMEOUT_S:
                timed_out_dataset_ids = [remaining_id for remaining_id in dataset_ids[index - 1 :] if remaining_id not in prepared_dataset_ids]
                if status_callback:
                    status_callback(
                        f"Regroup time budget reached during hint preparation after {self._format_elapsed_seconds(elapsed)}. "
                        f"Assigning the remaining {len(timed_out_dataset_ids)} dataset(s) to {REGROUP_OTHERS_GROUP_NAME}."
                    )
                break
            dataset = self.repository.get_dataset(dataset_id)
            if dataset is None:
                continue
            if status_callback:
                status_callback(
                    f"Preparing regroup input {index}/{total}: {dataset.display_name_user.strip() or dataset.default_name}"
                )
            datasets.append(self._dataset_for_regroup(dataset))
            prepared_dataset_ids.add(dataset.dataset_id)
            if progress_callback:
                progress_callback(int((index / max(total, 1)) * 35))

        if status_callback and datasets:
            status_callback(
                f"Fresh regroup input ready for {len(datasets)} dataset(s). "
                "User-entered names and descriptions were kept; cached AI grouping hints were ignored."
            )

        if not datasets and not timed_out_dataset_ids:
            return {
                "assignments": {},
                "dataset_ids": list(dataset_ids),
                "target_group_count": int(target_group_count),
            }

        assignments: dict[str, str] = {}
        if datasets:
            elapsed_before_grouping = monotonic() - regroup_started_at
            remaining_grouping_budget_s = max(0.0, REGROUP_TOTAL_TIMEOUT_S - elapsed_before_grouping)
            if status_callback:
                status_callback(
                    f"Starting group synthesis for {len(datasets)} dataset(s) with target {target_group_count} group(s)."
                )
            if progress_callback:
                progress_callback(45)
            if remaining_grouping_budget_s <= 0:
                if status_callback:
                    status_callback(
                        f"Regroup time budget was exhausted before the AI grouping step could start. "
                        f"Assigning {len(datasets) + len(timed_out_dataset_ids)} dataset(s) to {REGROUP_OTHERS_GROUP_NAME}."
                    )
                timed_out_dataset_ids.extend(dataset.dataset_id for dataset in datasets)
                datasets = []
            else:
                if status_callback:
                    status_callback(
                        f"Waiting for grouping response (max {self._format_elapsed_seconds(remaining_grouping_budget_s)} remaining). "
                        f"Any unassigned dataset(s) will be placed in {REGROUP_OTHERS_GROUP_NAME}."
                    )
                raw_assignments = self._group_datasets_for_regroup(
                    datasets,
                    target_group_count,
                    status_callback=status_callback,
                    timeout_s=remaining_grouping_budget_s,
                )
                if progress_callback:
                    progress_callback(70)
                covered_assignments = 0
                others_dataset_ids = set(timed_out_dataset_ids)
                for dataset in datasets:
                    proposed_group = str(raw_assignments.get(dataset.dataset_id) or "").strip()
                    normalized_group = sanitize_group_id(proposed_group)
                    if not proposed_group or normalized_group == "ungrouped":
                        others_dataset_ids.add(dataset.dataset_id)
                        continue
                    assignments[dataset.dataset_id] = proposed_group
                    covered_assignments += 1

                if status_callback:
                    if raw_assignments:
                        status_callback(
                            f"Grouping response covered {covered_assignments}/{len(datasets)} prepared dataset(s)."
                        )
                    else:
                        status_callback(
                            f"Grouping response did not return explicit assignments within the regroup budget. "
                            f"Prepared dataset(s) will fall back to {REGROUP_OTHERS_GROUP_NAME}."
                        )
                if not raw_assignments:
                    others_dataset_ids.update(dataset.dataset_id for dataset in datasets)
                if others_dataset_ids:
                    if status_callback:
                        status_callback(
                            f"Assigning {len(others_dataset_ids)} dataset(s) to {REGROUP_OTHERS_GROUP_NAME}."
                        )
                    for dataset_id in sorted(others_dataset_ids):
                        assignments[dataset_id] = REGROUP_OTHERS_GROUP_NAME
        if not datasets:
            if status_callback:
                status_callback(
                    f"No prepared datasets remained for synthesis. Assigning {len(timed_out_dataset_ids)} dataset(s) "
                    f"to {REGROUP_OTHERS_GROUP_NAME}."
                )
            assignments = {dataset_id: REGROUP_OTHERS_GROUP_NAME for dataset_id in timed_out_dataset_ids}

        return {
            "assignments": assignments,
            "dataset_ids": list(dataset_ids),
            "target_group_count": int(target_group_count),
        }

    def _dataset_for_regroup(self, dataset: DatasetRecord) -> DatasetRecord:
        return replace(
            dataset,
            display_name_ai="",
            description_ai="",
            suggested_group="",
            ai_confidence=0.0,
        )

    def _apply_regroup_assignments(
        self,
        assignments: dict[str, str],
        *,
        status_callback=None,
        progress_callback=None,
    ) -> int:
        if self.repository is None or not assignments:
            return 0
        if status_callback:
            status_callback(f"Applying {len(assignments)} group assignment(s) to the catalog.")
        if progress_callback:
            progress_callback(85)
        applied = self.repository.assign_groups_bulk(assignments)
        populated_group_ids = {
            stored.group_id
            for dataset_id in assignments
            if (stored := self.repository.get_dataset(dataset_id)) is not None and stored.group_id
        }
        if status_callback:
            status_callback(
                f"Regroup complete: {applied} dataset(s) assigned across {len(populated_group_ids)} populated group(s)."
            )
        if progress_callback:
            progress_callback(100)
        return applied

    def _regroup_dataset_ids(
        self,
        dataset_ids: list[str],
        target_group_count: int,
        *,
        status_callback=None,
        progress_callback=None,
    ) -> int:
        proposal = self._prepare_regroup_assignments(
            dataset_ids,
            target_group_count,
            status_callback=status_callback,
            progress_callback=progress_callback,
        )
        return self._apply_regroup_assignments(
            dict(proposal.get("assignments") or {}),
            status_callback=status_callback,
            progress_callback=progress_callback,
        )

    def _group_datasets_with_timeout(
        self,
        datasets: list,
        target_group_count: int,
        *,
        timeout_s: float | None,
    ) -> dict[str, str]:
        grouper = getattr(self.intelligence_service, "group_datasets", None)
        if not callable(grouper):
            return {}
        if timeout_s is None:
            return grouper(datasets, target_group_count)
        try:
            return grouper(datasets, target_group_count, timeout_s=timeout_s)
        except TypeError:
            return grouper(datasets, target_group_count)

    def _group_datasets_for_regroup(
        self,
        datasets: list,
        target_group_count: int,
        *,
        status_callback=None,
        timeout_s: float | None,
    ) -> dict[str, str]:
        classifier = getattr(self.intelligence_service, "classifier", None)
        availability_checker = getattr(classifier, "remote_availability_status", None)
        consume_last_error_message = getattr(classifier, "consume_last_error_message", None)
        broad_checker = getattr(classifier, "assignments_look_too_broad", None)
        if not callable(broad_checker):
            broad_checker = getattr(getattr(self.heuristic_intelligence_service, "classifier", None), "assignments_look_too_broad", None)

        remote_available = True
        remote_status_message = ""
        if callable(availability_checker):
            remote_available, remote_status_message = availability_checker()
            if status_callback and remote_status_message:
                if remote_available:
                    status_callback(f"{remote_status_message} Attempting remote AI grouping.")
                else:
                    status_callback(f"{remote_status_message} Using local grouping fallback.")

        grouping_started_at = monotonic()

        def _remaining_timeout() -> float | None:
            if timeout_s is None:
                return None
            return max(0.0, timeout_s - (monotonic() - grouping_started_at))

        def _run_grouping(target: int) -> dict[str, str]:
            effective_timeout_s = _remaining_timeout()
            if not remote_available:
                return self.heuristic_intelligence_service.group_datasets(
                    datasets,
                    target,
                    timeout_s=effective_timeout_s,
                )
            return self._group_datasets_with_timeout(
                datasets,
                target,
                timeout_s=effective_timeout_s,
            )

        def _consume_grouping_error() -> None:
            if callable(consume_last_error_message):
                last_error = str(consume_last_error_message() or "").strip()
                if last_error and status_callback:
                    status_callback(f"{last_error} Using local grouping fallback where needed.")

        def _group_count(group_assignments: dict[str, str]) -> int:
            return len({str(name).strip() for name in group_assignments.values() if str(name).strip()})

        assignments = _run_grouping(target_group_count)
        _consume_grouping_error()

        if not assignments or not callable(broad_checker):
            return assignments
        current_target = int(target_group_count)
        current_group_count = _group_count(assignments)
        current_still_broad = broad_checker(datasets, assignments, current_target)
        if not current_still_broad:
            return assignments
        best_assignments = assignments
        best_group_count = current_group_count
        best_target = current_target
        best_is_broad = current_still_broad
        attempt_count = 1

        while best_is_broad and attempt_count < 3:
            retry_target = self._suggest_regroup_retry_target(len(datasets), best_target)
            if retry_target <= best_target:
                break
            remaining_timeout_s = _remaining_timeout()
            if remaining_timeout_s is not None and remaining_timeout_s < 5.0:
                if status_callback:
                    status_callback(
                        "Grouping result still looks too broad, but there is not enough regroup time left to retry with more groups."
                    )
                break
            if status_callback:
                status_callback(
                    f"Grouping result still looks too broad at target {best_target} group(s). "
                    f"Retrying with suggested target {retry_target} group(s)."
                )
            retry_assignments = _run_grouping(retry_target)
            _consume_grouping_error()
            if not retry_assignments:
                if status_callback:
                    status_callback("Retry with more groups returned no assignments. Keeping the best earlier grouping result.")
                break

            attempt_count += 1
            retry_group_count = _group_count(retry_assignments)
            retry_still_broad = broad_checker(datasets, retry_assignments, retry_target)
            if not retry_still_broad:
                if status_callback:
                    status_callback(f"Using regroup retry result with target {retry_target} group(s).")
                return retry_assignments

            if retry_group_count > best_group_count:
                best_assignments = retry_assignments
                best_group_count = retry_group_count
                best_target = retry_target
                best_is_broad = True
                if status_callback:
                    status_callback(
                        f"Retry at target {retry_target} improved coverage to {retry_group_count} group(s), "
                        "but it still looks too broad."
                    )
                continue

            if status_callback:
                status_callback(
                    f"Retry at target {retry_target} did not improve the grouping spread beyond {best_group_count} group(s)."
                )
            break

        if status_callback and best_is_broad:
            status_callback(
                f"Grouping still looks too broad after {attempt_count} AI attempt(s). "
                f"Keeping the best available result with {best_group_count} group(s) for review."
            )
        return best_assignments

    def on_background_error(self, message: str, progress_token: int | None = None) -> None:
        self.append_activity_log(message)
        self.review_job_status.setText("Background job failed.")
        self._review_job_running = False
        if progress_token is not None:
            self._finish_background_activity(progress_token, "Background job failed.")
            self._release_worker(progress_token)
        QMessageBox.warning(self, "Background job failed", message.splitlines()[-1] if message else "Unknown error")

    def _on_review_job_finished(self, success_message: str) -> None:
        self._review_job_running = False
        self.review_job_status.setText(success_message)
        if self._map_refresh_pending and self._is_map_tab_active():
            self.refresh_map()

    def _refresh_all_views_after_worker(self, activity: str) -> None:
        self.append_activity_log("Applying results to the catalog and refreshing views.", activity=activity)
        self.refresh_all_views()

    def _schedule_refresh_all_views_after_worker(self, activity: str) -> None:
        QTimer.singleShot(0, lambda activity=activity: self._refresh_all_views_after_worker(activity))

    def _complete_worker_success(
        self,
        token: int,
        success_message: str,
        *,
        activity: str,
        refresh_after: bool,
        track_review_job: bool,
    ) -> None:
        def _finalize_success() -> None:
            try:
                if refresh_after:
                    self._update_background_activity_status(token, "Applying results to the catalog and refreshing views.")
                    self._refresh_all_views_after_worker(activity)
                self._finish_background_activity(token, success_message)
                self.statusBar().showMessage(success_message, 5000)
                if track_review_job:
                    self._on_review_job_finished(success_message)
            except Exception:
                self.on_background_error(traceback.format_exc(), token)

        if refresh_after:
            QTimer.singleShot(0, _finalize_success)
        else:
            _finalize_success()

    def _release_worker(self, token: int) -> None:
        self._active_workers.pop(token, None)

    def _begin_background_activity(self, label: str, activity: str | None = None) -> int:
        self._background_progress_token += 1
        token = self._background_progress_token
        self._active_background_progress_token = token
        activity_name = self._resolve_log_activity(activity or label)
        self._background_activity_names[token] = activity_name
        self._background_activity_last_status[token] = ""
        self._background_activity_progress_value = None
        self._background_activity_started_at = monotonic()
        self._background_activity_worker_signal_at = self._background_activity_started_at
        self._background_heartbeat_timer.start()
        self._set_log_button_live(True)
        self.append_activity_log("starting", activity=activity_name)
        return token

    def _update_background_activity_status(self, token: int, label: str) -> None:
        if token != self._active_background_progress_token:
            return
        self._background_activity_last_status[token] = str(label or "").strip()
        self._background_activity_worker_signal_at = monotonic()

    def _update_background_activity_progress(self, token: int, value: int) -> None:
        if token != self._active_background_progress_token:
            return
        self._background_activity_progress_value = max(0, min(100, int(value)))
        self._background_activity_worker_signal_at = monotonic()
        self._set_log_button_live(True)

    def _finish_background_activity(self, token: int, label: str) -> None:
        if token != self._active_background_progress_token:
            return
        activity_name = self._background_activity_names.get(token, self._resolve_log_activity())
        self._background_activity_progress_value = 100
        self.append_activity_log("ending", activity=activity_name)
        self._background_activity_names.pop(token, None)
        self._background_activity_last_status.pop(token, None)
        self._release_worker(token)
        self._active_background_progress_token = 0
        self._background_activity_progress_value = None
        self._background_activity_started_at = None
        self._background_activity_worker_signal_at = None
        self._background_heartbeat_timer.stop()
        self._release_worker(token)
        self._set_log_button_live(False)

    def _build_log_window(self) -> None:
        self.log_window = QWidget(self, Qt.Window)
        self.log_window.setWindowTitle(f"{APP_DISPLAY_NAME} Activity Log")
        self.log_window.resize(860, 420)
        layout = QVBoxLayout(self.log_window)
        self.log_text = QPlainTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setPlaceholderText("Background activity log")
        layout.addWidget(self.log_text, 1)

    def open_log_window(self) -> None:
        self.log_window.show()
        if hasattr(self.log_window, "raise_"):
            self.log_window.raise_()
        if hasattr(self.log_window, "activateWindow"):
            self.log_window.activateWindow()

    def _apply_canvas_theme(self) -> None:
        checked_icon = (UI_ASSETS_DIR / "checkmark_checked.svg").as_posix()
        indeterminate_icon = (UI_ASSETS_DIR / "checkmark_indeterminate.svg").as_posix()
        self.setStyleSheet(
            """
            QWidget#CentralHost {
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 1, y2: 1,
                    stop: 0 #efe7d8,
                    stop: 0.55 #f5efe3,
                    stop: 1 #e8ddc7
                );
            }
            QTabWidget::pane {
                border-top: 1px solid #cbb791;
                background-color: #f3ecdf;
            }
            QWidget#ImportTab, QWidget#ReviewDatasetsTab, QWidget#InfoSourcesTab,
            QWidget#DatasetsOverviewTab, QWidget#MapTab, QWidget#SettingsTab, QWidget#AboutTab {
                background-color: #f3ecdf;
            }
            QWidget#GlobalActionsBar {
                background: transparent;
            }
            QTabBar::tab {
                background-color: #e6dac2;
                color: #5c4a2f;
                border: 1px solid #c6b089;
                border-bottom: none;
                border-top-left-radius: 5px;
                border-top-right-radius: 5px;
                padding: 6px 12px;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background-color: #f8f3e9;
            }
            QTabBar::tab:!selected {
                margin-top: 2px;
            }
            QGroupBox {
                background-color: #faf6ee;
                border: 1px solid #d5c3a4;
                border-radius: 8px;
                margin-top: 12px;
                padding-top: 10px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 4px;
                color: #715a36;
            }
            QLabel {
                color: #3f3528;
            }
            QLineEdit, QComboBox, QTextEdit, QPlainTextEdit, QTableWidget, QTreeWidget {
                background-color: #fffdf8;
                border: 1px solid #d9cab1;
                border-radius: 6px;
            }
            QTreeView, QTreeWidget, QTableView, QTableWidget {
                show-decoration-selected: 1;
                selection-background-color: #d7bb7f;
                selection-color: #2f2517;
            }
            QTableWidget, QTreeWidget {
                alternate-background-color: #f6efdf;
                gridline-color: #e2d5bf;
            }
            QTreeWidget::item:selected,
            QTreeWidget::item:selected:active,
            QTreeWidget::item:selected:!active,
            QTableWidget::item:selected,
            QTableWidget::item:selected:active,
            QTableWidget::item:selected:!active {
                background-color: #d7bb7f;
                color: #2f2517;
            }
            QTreeWidget::item:hover,
            QTableWidget::item:hover {
                background-color: #efe1bf;
                color: #2f2517;
            }
            QCheckBox::indicator,
            QTreeView::indicator,
            QTreeWidget::indicator {
                width: 15px;
                height: 15px;
                border: 1px solid #6b5027;
                border-radius: 3px;
                background-color: #e7d1a6;
            }
            QCheckBox::indicator:unchecked,
            QTreeView::indicator:unchecked,
            QTreeWidget::indicator:unchecked {
                border: 1px solid #684d24;
                background-color: #dfc792;
            }
            QCheckBox::indicator:checked,
            QTreeView::indicator:checked,
            QTreeWidget::indicator:checked {
                border: 1px solid #513912;
                background-color: #9a7230;
                image: url("%s");
            }
            QCheckBox::indicator:indeterminate,
            QTreeView::indicator:indeterminate,
            QTreeWidget::indicator:indeterminate {
                border: 1px solid #513912;
                background-color: #b28945;
                image: url("%s");
            }
            QHeaderView::section {
                background-color: #e7dbc4;
                color: #54462d;
                border: 1px solid #cfbc99;
                padding: 4px 6px;
            }
            QPushButton {
                background-color: #f0e3cb;
                border: 1px solid #c8b088;
                border-radius: 5px;
                padding: 5px 10px;
                color: #4d4029;
            }
            QPushButton:hover {
                background-color: #e9d9b8;
            }
            QPushButton:pressed {
                background-color: #dfcca2;
            }
            QPushButton[liveLog="true"] {
                background-color: #d8ba7a;
                font-weight: 600;
            }
            QPushButton#CornerLogButton,
            QPushButton#CornerExitButton {
                min-height: 22px;
                max-height: 24px;
                padding: 2px 8px;
                border-radius: 4px;
                margin: 0;
                background-color: #ede0c7;
                border: 1px solid #bda273;
            }
            QPushButton#CornerExitButton {
                background-color: #eadfc8;
                border: 1px solid #b79f73;
                color: #453621;
            }
            QPushButton#CornerExitButton:hover {
                background-color: #e1d1ae;
            }
            QPushButton#CornerExitButton:pressed {
                background-color: #d4c094;
            }
            QProgressBar {
                background-color: #f8f2e7;
                border: 1px solid #d3c29f;
                border-radius: 5px;
                text-align: center;
                color: #4f4129;
            }
            QProgressBar::chunk {
                background-color: #b79b67;
                border-radius: 4px;
            }
            QStatusBar {
                background-color: #eee4d2;
                border-top: 1px solid #cfbc99;
            }
            """
            % (checked_icon, indeterminate_icon)
        )

    def _set_log_button_live(self, is_live: bool) -> None:
        if not hasattr(self, "log_button"):
            return
        if is_live:
            progress_suffix = ""
            if self._background_activity_progress_value is not None:
                progress_suffix = f" {self._background_activity_progress_value}%"
            self.log_button.setText(f"Logs*{progress_suffix}")
        else:
            self.log_button.setText("Logs")
        tooltip = "Open the global activity log."
        if is_live:
            activity_name = self._background_activity_names.get(self._active_background_progress_token)
            if activity_name:
                tooltip += f" Active: {activity_name}"
                if self._background_activity_progress_value is not None:
                    tooltip += f" ({self._background_activity_progress_value}%)"
        self.log_button.setToolTip(tooltip)
        self.log_button.setProperty("liveLog", is_live)
        self.log_button.style().unpolish(self.log_button)
        self.log_button.style().polish(self.log_button)
        self.log_button.update()

    def _emit_background_activity_heartbeat(self) -> None:
        token = self._active_background_progress_token
        if not token:
            self._background_heartbeat_timer.stop()
            return
        activity_name = self._background_activity_names.get(token, self._resolve_log_activity())
        last_status = self._background_activity_last_status.get(token, "").strip()
        started_at = self._background_activity_started_at
        if started_at is None:
            elapsed_note = "still running"
        else:
            elapsed_seconds = max(0, int(monotonic() - started_at))
            elapsed_note = f"still running ({self._format_elapsed_seconds(elapsed_seconds)} elapsed)"
        if last_status:
            elapsed_note = f"{elapsed_note}; latest step: {last_status}"
        self.append_activity_log(elapsed_note, activity=activity_name)

    def _format_elapsed_seconds(self, value: float | int) -> str:
        total_seconds = max(0, int(value))
        minutes, seconds = divmod(total_seconds, 60)
        return f"{minutes:02d}:{seconds:02d}"

    def _resolve_log_activity(self, activity: str | None = None) -> str:
        value = activity
        if not value and self._active_background_progress_token:
            value = self._background_activity_names.get(self._active_background_progress_token)
        cleaned = (value or "Application").strip().rstrip(".")
        return cleaned or "Application"

    def _ensure_review_job_can_start(self) -> bool:
        self._clear_stale_review_job_lock_if_needed()
        if not self._review_job_running:
            return True
        activity_name = self._background_activity_names.get(self._active_background_progress_token, "Background job")
        QMessageBox.information(
            self,
            "Background job running",
            f"Wait for the current job to finish first: {activity_name}.",
        )
        return False

    def _clear_stale_review_job_lock_if_needed(self) -> bool:
        if not self._review_job_running:
            return False
        token = self._active_background_progress_token
        if not token:
            self._review_job_running = False
            return True
        last_signal_at = self._background_activity_worker_signal_at
        if last_signal_at is None:
            self._review_job_running = False
            return True
        idle_seconds = monotonic() - last_signal_at
        if idle_seconds < REVIEW_JOB_STALE_LOCK_TIMEOUT_S:
            return False
        activity_name = self._background_activity_names.get(token, self._resolve_log_activity())
        self.append_activity_log(
            f"Cleared a stale review-job lock after {self._format_elapsed_seconds(idle_seconds)} without worker updates.",
            activity=activity_name,
        )
        self._background_activity_names.pop(token, None)
        self._background_activity_last_status.pop(token, None)
        self._active_background_progress_token = 0
        self._background_activity_progress_value = None
        self._background_activity_started_at = None
        self._background_activity_worker_signal_at = None
        self._background_heartbeat_timer.stop()
        self._set_log_button_live(False)
        self._review_job_running = False
        self.review_job_status.setText("Recovered from a stale background job lock.")
        return True

    def _format_log_entry(self, detail: str, activity: str | None = None) -> str:
        timestamp = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        return f"[{timestamp}] [{self._resolve_log_activity(activity)}] - {detail}"

    def _write_activity_log_entries(self, entries: list[str]) -> None:
        if not entries or self.current_workspace is None:
            return
        path = self.current_workspace.activity_log_path()
        try:
            with path.open("a", encoding="utf-8") as handle:
                for entry in entries:
                    handle.write(f"{entry}\n")
        except OSError:
            return

    def _load_activity_log(self) -> None:
        if not hasattr(self, "log_text"):
            return
        if self.current_workspace is None:
            self.log_text.setPlainText("")
            return
        path = self.current_workspace.activity_log_path()
        if not path.exists():
            self.log_text.setPlainText("")
            return
        try:
            self.log_text.setPlainText(path.read_text(encoding="utf-8"))
        except OSError:
            self.log_text.setPlainText("")

