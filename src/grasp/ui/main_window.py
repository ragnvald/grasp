from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shutil
import traceback
from time import monotonic, sleep

from grasp.branding import (
    APP_ACRONYM,
    APP_AUTHOR,
    APP_DISPLAY_NAME,
    APP_MISSION,
    APP_TAGLINE,
    APP_WINDOW_TITLE,
    DEFAULT_EXPORT_GPKG_NAME,
    DEFAULT_EXPORT_PARQUET_NAME,
)
from grasp.catalog.repository import CatalogRepository
from grasp.export.service import ExportService
from grasp.ingest.service import IngestService, MAX_AUTO_VISIBLE_DATASETS, MAX_AUTO_VISIBLE_FEATURES
from grasp.intelligence.providers import (
    DEFAULT_OPENAI_MODEL,
    DuckDuckGoSearchProvider,
    HeuristicClassificationProvider,
    OpenAIClassificationProvider,
)
from grasp.intelligence.service import IntelligenceService, SearchService
from grasp.qt_compat import (
    QAction,
    QApplication,
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
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
    QSplitter,
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
    QWebEngineSettings,
    QWebEngineView,
    Signal,
    WEBENGINE_AVAILABLE,
)
from grasp.ui.map_bridge import MapBridge
from grasp.ui.settings_dialog import MODEL_OPTIONS
from grasp.ui.workers import FunctionWorker
from grasp.settings import AppSettings, SettingsStore
from grasp.styling import StyleService
from grasp.workspace import catalog_exists, ensure_workspace, sanitize_group_id


REGROUP_OTHERS_GROUP_NAME = "Others"
REGROUP_HINT_PREPARATION_TIMEOUT_S = 120.0
REGROUP_TOTAL_TIMEOUT_S = 120.0
INITIAL_HEURISTIC_CLASSIFICATION_TIMEOUT_S = 60.0
REVIEW_JOB_STALE_LOCK_TIMEOUT_S = 300.0
REMOTE_AI_REQUEST_COOLDOWN_S = 0.35
UNDERSTANDING_PERSIST_BATCH_SIZE = 24
MAP_HTTP_USER_AGENT = "GRASP-Desktop (+https://github.com/ragnvald/grasp)"


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
        self._checked_dataset_id_set: set[str] = set()
        self._map_initialized = False
        self._map_refresh_pending = False
        self._review_job_running = False
        self._background_progress_token = 0
        self._active_background_progress_token = 0
        self._background_activity_names: dict[int, str] = {}
        self._background_activity_last_status: dict[int, str] = {}
        self._background_activity_progress_value: int | None = None
        self._background_activity_started_at: float | None = None
        self._background_activity_worker_signal_at: float | None = None
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
        self.tabs.currentChanged.connect(self.on_tab_changed)
        central_layout.addWidget(self.tabs, 1)
        self.setCentralWidget(central_host)

        self.import_tab = QWidget()
        self.import_tab.setObjectName("ImportTab")
        self.review_tab = QWidget()
        self.review_tab.setObjectName("ReviewTab")
        self.map_tab = QWidget()
        self.map_tab.setObjectName("MapTab")
        self.settings_tab = QWidget()
        self.settings_tab.setObjectName("SettingsTab")
        self.about_tab = QWidget()
        self.about_tab.setObjectName("AboutTab")

        self.tabs.addTab(self.import_tab, "Import")
        self.tabs.addTab(self.review_tab, "Review")
        self.tabs.addTab(self.map_tab, "Map / Export")
        self.tabs.addTab(self.settings_tab, "Settings")
        self.tabs.addTab(self.about_tab, "About")

        self._build_import_tab()
        self._build_review_tab()
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

        self.load_existing_action = QAction("Load Existing", self)
        self.load_existing_action.triggered.connect(self.load_existing_catalog)
        file_menu.addAction(self.load_existing_action)

        scan_action = QAction("Load from folder", self)
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

        row = QHBoxLayout()
        self.folder_edit = QLineEdit()
        self.folder_edit.setPlaceholderText("Choose an input folder with GIS vector data")
        if self.current_settings.last_folder:
            self.folder_edit.setText(self.current_settings.last_folder)
        self.folder_edit.textChanged.connect(self.on_folder_changed)
        row.addWidget(self.folder_edit, 1)

        self.browse_button = QPushButton("Browse")
        self.browse_button.clicked.connect(self.browse_folder)
        row.addWidget(self.browse_button)

        self.scan_button = QPushButton("Load from folder")
        self.scan_button.clicked.connect(self.start_scan)
        row.addWidget(self.scan_button)

        self.load_existing_button = QPushButton("Load Existing")
        self.load_existing_button.clicked.connect(self.load_existing_catalog)
        row.addWidget(self.load_existing_button)

        self.reset_data_button = QPushButton("Reset All Data")
        self.reset_data_button.clicked.connect(self.reset_all_data)
        row.addWidget(self.reset_data_button)
        layout.addLayout(row)

        self.import_summary = QLabel("No folder loaded.")
        layout.addWidget(self.import_summary)

        self.import_progress = QProgressBar()
        self.import_progress.setRange(0, 100)
        layout.addWidget(self.import_progress)

        self.import_table = QTableWidget(0, 5)
        self.import_table.setHorizontalHeaderLabels(["Name", "Format", "Geometry", "Features", "Source"])
        self.import_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.import_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.import_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.import_table.setSortingEnabled(True)
        self.import_table.horizontalHeader().setSortIndicatorShown(True)
        self.import_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.import_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        layout.addWidget(self.import_table, 1)

        self.import_log_note = QLabel(
            "Background activity is written to the global activity log. "
            "Use Logs in the top-right corner to open it, and watch the button for live progress."
        )
        self.import_log_note.setWordWrap(True)
        layout.addWidget(self.import_log_note)
        self._update_folder_actions()

    def _build_review_tab(self) -> None:
        layout = QVBoxLayout(self.review_tab)

        self.review_actions_group_box = QGroupBox("Info & Sources")
        review_actions_box_layout = QVBoxLayout(self.review_actions_group_box)
        review_actions_layout = QHBoxLayout()
        self.fast_info_button = QPushButton("Find info (fast)")
        self.fast_info_button.clicked.connect(self.start_fast_info_for_scope)
        review_actions_layout.addWidget(self.fast_info_button)

        self.run_ai_sources_button = QPushButton("Find info (AI)")
        self.run_ai_sources_button.clicked.connect(self.start_ai_for_scope)
        review_actions_layout.addWidget(self.run_ai_sources_button)

        self.find_sources_button = QPushButton("Find sources")
        self.find_sources_button.clicked.connect(self.start_sources_for_scope)
        review_actions_layout.addWidget(self.find_sources_button)

        review_actions_layout.addWidget(QLabel("Scope"))
        self.review_scope_combo = QComboBox()
        self.review_scope_combo.addItem("Checked datasets", "checked")
        self.review_scope_combo.addItem("All datasets", "all")
        review_actions_layout.addWidget(self.review_scope_combo)
        review_actions_layout.addStretch(1)
        review_actions_box_layout.addLayout(review_actions_layout)

        self.review_actions_note = QLabel(
            "Find info (fast) runs a local first-pass without external AI. "
            "Find info (AI) updates AI title, AI description and AI group. "
            "Find sources keeps the current understanding and refreshes source lookup only."
        )
        self.review_actions_note.setWordWrap(True)
        review_actions_box_layout.addWidget(self.review_actions_note)
        layout.addWidget(self.review_actions_group_box)

        self.review_job_status = QLabel("No dataset processing job running.")
        self.review_job_status.setWordWrap(True)
        layout.addWidget(self.review_job_status)

        self.review_visibility_note = QLabel(
            "Checked datasets are selected for batch actions in Review. "
            "Visible on map is controlled separately in the dataset panel on the right. On import, the app auto-enables map visibility for up to "
            f"{MAX_AUTO_VISIBLE_DATASETS} smaller layers (max {MAX_AUTO_VISIBLE_FEATURES} features each) "
            "and leaves the rest off to keep the map responsive. "
            "Use Select/Clear All or Select/Clear Group to change many layers quickly."
        )
        self.review_visibility_note.setWordWrap(True)

        self.review_progress = QProgressBar()
        self.review_progress.setRange(0, 100)
        self.review_progress.setValue(0)
        layout.addWidget(self.review_progress)

        splitter = QSplitter()
        layout.addWidget(splitter, 1)

        datasets_host = QWidget()
        datasets_host_layout = QVBoxLayout(datasets_host)
        datasets_host_layout.setContentsMargins(0, 0, 0, 0)

        self.datasets_group_box = QGroupBox("Datasets")
        datasets_group_layout = QVBoxLayout(self.datasets_group_box)
        datasets_group_layout.addWidget(self.review_visibility_note)

        self.selection_group_box = QGroupBox("Selection")
        selection_layout = QHBoxLayout(self.selection_group_box)
        self.show_all_button = QPushButton("Select All")
        self.show_all_button.clicked.connect(lambda: self.set_all_checked(True))
        selection_layout.addWidget(self.show_all_button)

        self.hide_all_button = QPushButton("Clear All")
        self.hide_all_button.clicked.connect(lambda: self.set_all_checked(False))
        selection_layout.addWidget(self.hide_all_button)

        self.show_group_button = QPushButton("Select Group")
        self.show_group_button.clicked.connect(lambda: self.set_selected_group_checked(True))
        selection_layout.addWidget(self.show_group_button)

        self.hide_group_button = QPushButton("Clear Group")
        self.hide_group_button.clicked.connect(lambda: self.set_selected_group_checked(False))
        selection_layout.addWidget(self.hide_group_button)
        selection_layout.addStretch(1)
        datasets_group_layout.addWidget(self.selection_group_box)

        self.grouping_group_box = QGroupBox("Grouping")
        grouping_layout = QHBoxLayout(self.grouping_group_box)
        self.new_group_button = QPushButton("New Group")
        self.new_group_button.clicked.connect(self.create_group)
        grouping_layout.addWidget(self.new_group_button)

        self.apply_group_button = QPushButton("Apply Suggested Group")
        self.apply_group_button.clicked.connect(self.apply_suggested_group)
        grouping_layout.addWidget(self.apply_group_button)

        self.regroup_button = QPushButton("AI Regroup...")
        self.regroup_button.clicked.connect(self.start_regroup_for_scope)
        grouping_layout.addWidget(self.regroup_button)

        grouping_layout.addWidget(QLabel("Scope"))
        self.grouping_scope_combo = QComboBox()
        self.grouping_scope_combo.addItem("Checked datasets", "checked")
        self.grouping_scope_combo.addItem("All datasets", "all")
        grouping_layout.addWidget(self.grouping_scope_combo)
        grouping_layout.addStretch(1)
        datasets_group_layout.addWidget(self.grouping_group_box)

        self.dataset_actions_group_box = QGroupBox("Selection actions")
        dataset_actions_layout = QHBoxLayout(self.dataset_actions_group_box)
        self.fill_ai_fields_button = QPushButton("Fill Empty Fields from AI")
        self.fill_ai_fields_button.clicked.connect(self.fill_checked_user_fields_from_ai)
        dataset_actions_layout.addWidget(self.fill_ai_fields_button)

        self.make_visible_button = QPushButton("Make visible in maps")
        self.make_visible_button.clicked.connect(self.make_checked_visible_in_maps)
        dataset_actions_layout.addWidget(self.make_visible_button)

        self.include_in_report_button = QPushButton("Include in report")
        self.include_in_report_button.clicked.connect(self.include_checked_in_report)
        dataset_actions_layout.addWidget(self.include_in_report_button)
        dataset_actions_layout.addStretch(1)
        datasets_group_layout.addWidget(self.dataset_actions_group_box)

        self.tree = DatasetTreeWidget()
        self.tree.setHeaderLabels(["Datasets"])
        self.tree.setDragDropMode(QTreeWidget.InternalMove)
        self.tree.setSelectionMode(QTreeWidget.SingleSelection)
        self.tree.itemSelectionChanged.connect(self.on_tree_selection_changed)
        self.tree.itemChanged.connect(self.on_tree_item_changed)
        self.tree.orderingChanged.connect(self.on_tree_order_changed)
        datasets_group_layout.addWidget(self.tree, 1)

        datasets_host_layout.addWidget(self.datasets_group_box, 1)
        splitter.addWidget(datasets_host)

        inspector_host = QWidget()
        inspector_layout = QVBoxLayout(inspector_host)

        dataset_group = QGroupBox("Dataset")
        dataset_form = QFormLayout(dataset_group)
        self.dataset_name_edit = QLineEdit()
        dataset_form.addRow("Name", self.dataset_name_edit)

        self.dataset_description_edit = QTextEdit()
        self.dataset_description_edit.setMinimumHeight(72)
        self.dataset_description_edit.setMaximumHeight(96)
        dataset_form.addRow("Description", self.dataset_description_edit)

        self.visibility_checkbox = QCheckBox("Visible on map")
        self.include_export_checkbox = QCheckBox("Include in export")
        dataset_form.addRow("", self.visibility_checkbox)
        dataset_form.addRow("", self.include_export_checkbox)

        self.source_path_label = QLabel("-")
        self.source_path_label.setWordWrap(True)
        dataset_form.addRow("Source", self.source_path_label)

        self.geometry_label = QLabel("-")
        dataset_form.addRow("Geometry", self.geometry_label)

        self.feature_count_label = QLabel("-")
        dataset_form.addRow("Features", self.feature_count_label)

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

        inspector_layout.addWidget(dataset_group)
        inspector_layout.addStretch(1)
        splitter.addWidget(inspector_host)
        splitter.setStretchFactor(0, 3)
        splitter.setStretchFactor(1, 4)

    def _build_map_tab(self) -> None:
        layout = QVBoxLayout(self.map_tab)

        row = QHBoxLayout()
        refresh_button = QPushButton("Refresh Map")
        refresh_button.clicked.connect(self.refresh_map)
        row.addWidget(refresh_button)

        self.generate_styles_button = QPushButton("Generate Styles")
        self.generate_styles_button.clicked.connect(self.start_style_for_scope)
        row.addWidget(self.generate_styles_button)

        row.addWidget(QLabel("Scope"))
        self.map_scope_combo = QComboBox()
        self.map_scope_combo.addItem("Visible on map", "visible")
        self.map_scope_combo.addItem("Show all", "all")
        self.map_scope_combo.currentIndexChanged.connect(lambda _index: self.refresh_map())
        row.addWidget(self.map_scope_combo)

        export_gpkg_button = QPushButton("Export GeoPackage")
        export_gpkg_button.clicked.connect(self.export_gpkg)
        row.addWidget(export_gpkg_button)

        export_parquet_button = QPushButton("Export GeoParquet")
        export_parquet_button.clicked.connect(self.export_geoparquet)
        row.addWidget(export_parquet_button)
        row.addStretch(1)
        layout.addLayout(row)

        self.map_summary = QLabel("No project loaded.")
        self.map_summary.setWordWrap(True)
        layout.addWidget(self.map_summary)

        self.map_style_note = QLabel(
            "Generate styles from dataset names and descriptions when you want a coherent map preview and QGIS-ready export."
        )
        self.map_style_note.setWordWrap(True)
        layout.addWidget(self.map_style_note)

        if WEBENGINE_AVAILABLE:
            self.map_view = QWebEngineView()
        else:
            self.map_view = QPlainTextEdit()
            self.map_view.setReadOnly(True)
            self.map_view.setPlainText("Qt WebEngine is not available in this environment.")
        layout.addWidget(self.map_view, 1)

    def _build_settings_tab(self) -> None:
        layout = QVBoxLayout(self.settings_tab)

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
        ai_intro = QLabel("Settings used for manual OpenAI-based title, description, grouping, and source ranking.")
        ai_intro.setWordWrap(True)
        ai_group_layout.addWidget(ai_intro)

        ai_form = QFormLayout()
        self.settings_model_combo = QComboBox()
        self.settings_model_combo.setEditable(True)
        self.settings_model_combo.addItems(MODEL_OPTIONS)
        ai_form.addRow("OpenAI model", self.settings_model_combo)

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
            "Choose which dataset clues are sent to OpenAI during manual AI runs in Review. "
            "Keep this lean to reduce token use. Search-based enrichment can add more evidence later."
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

        self.about_author_label = QLabel(f"Created by {APP_AUTHOR}")
        self.about_author_label.setWordWrap(True)
        layout.addWidget(self.about_author_label)

        self.about_purpose_label = QLabel(
            "GRASP is a desktop application for working through folders of GIS vector files such as "
            "Shapefile, GeoPackage and GeoParquet datasets."
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
            "- exports a packaged GeoPackage with metadata and QGIS project information"
        )
        self.about_capabilities_label.setWordWrap(True)
        layout.addWidget(self.about_capabilities_label)

        self.about_note_label = QLabel(
            "The goal is to help users build as much structured knowledge as possible around their spatial datasets "
            "before packaging them for further GIS work."
        )
        self.about_note_label.setWordWrap(True)
        layout.addWidget(self.about_note_label)

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
        self.append_activity_log(f"Loaded existing catalog from {workspace.db_path}", activity="Load Existing")
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
        self.append_activity_log(f"Loading datasets from {folder}", activity="Load from folder")
        existing_records = self.repository.list_datasets() if self.repository is not None else []
        worker = FunctionWorker(self.ingest_service.scan_folder, folder, existing_records)
        progress_token = self._begin_background_activity("Loading from folder...", activity="Load from folder")
        worker.signals.status.connect(lambda message: self.append_activity_log(message, activity="Load from folder"))
        worker.signals.progress.connect(self.import_progress.setValue)
        worker.signals.status.connect(lambda message, token=progress_token: self._update_background_activity_status(token, message))
        worker.signals.progress.connect(lambda value, token=progress_token: self._update_background_activity_progress(token, value))
        worker.signals.result.connect(self._schedule_scan_result)
        worker.signals.error.connect(lambda message, token=progress_token: self.on_background_error(message, token))
        worker.signals.finished.connect(lambda token=progress_token: self._finish_background_activity(token, "Load from folder finished."))
        worker.signals.finished.connect(lambda: self.statusBar().showMessage("Load from folder finished.", 5000))
        self.thread_pool.start(worker)

    def _schedule_scan_result(self, datasets) -> None:
        QTimer.singleShot(0, lambda datasets=datasets: self.on_scan_result(datasets))

    def on_scan_result(self, datasets) -> None:
        if self.repository is None:
            return
        sync_summary = self.repository.replace_datasets(datasets)
        self.append_activity_log(f"Persisted {len(datasets)} dataset(s) to local catalog.", activity="Load from folder")
        if sync_summary["reused_ids"]:
            self.append_activity_log(
                f"Reused {len(sync_summary['reused_ids'])} unchanged dataset(s) from the existing catalog.",
                activity="Load from folder",
            )
        if sync_summary["removed_ids"]:
            self.append_activity_log(
                f"Removed {len(sync_summary['removed_ids'])} dataset(s) no longer present in the source folder.",
                activity="Load from folder",
            )
        self.append_activity_log("Applying loaded datasets to the catalog and refreshing views.", activity="Load from folder")
        self.refresh_all_views()
        dataset_ids = sync_summary["changed_ids"]
        if dataset_ids:
            self.append_activity_log(
                f"{len(dataset_ids)} new or changed dataset(s) are ready for Find info (fast) in Review.",
                activity="Load from folder",
            )
        else:
            self.append_activity_log(
                "No new or changed datasets detected. Existing AI understanding and sources were kept.",
                activity="Load from folder",
            )

    def save_settings(self) -> None:
        try:
            timeout_s = float(self.settings_timeout_edit.text().strip() or "20")
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
            openai_model=self.settings_model_combo.currentText().strip() or MODEL_OPTIONS[0],
            openai_api_key=self.settings_api_key_edit.text().strip(),
            openai_endpoint=self.settings_endpoint_edit.text().strip(),
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
        self._map_refresh_pending = False
        self._background_activity_names.clear()
        self._active_background_progress_token = 0
        self._background_activity_started_at = None
        self._background_heartbeat_timer.stop()
        self.import_table.setRowCount(0)
        self.import_summary.setText("No folder loaded.")
        self.tree.clear()
        self.populate_inspector(None)
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
            self.repository.create_group(name.strip())
            self.refresh_tree()

    def start_ai_selected(self) -> None:
        self.review_scope_combo.setCurrentIndex(self.review_scope_combo.findData("checked"))
        self.start_ai_for_scope()

    def start_fast_info_for_scope(self) -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(self._review_scope())
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose dataset",
                "Check one or more datasets or switch scope to All datasets first.",
            )
            return
        self.append_activity_log(
            "Fast local classification runs without external AI and is intended as a quick first pass.",
            activity="Fast Local Classification",
        )
        self._run_review_job_foreground_with_refresh(
            self._heuristic_classify_dataset_ids,
            dataset_ids,
            "Fast local classification completed.",
            start_message="Running fast local classification without external AI (max 1 minute)...",
            activity_name="Fast Local Classification",
        )

    def start_ai_for_scope(self) -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(self._review_scope())
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose dataset",
                "Check one or more datasets or switch scope to All datasets first.",
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
        self.review_scope_combo.setCurrentIndex(self.review_scope_combo.findData("all"))
        self.start_ai_for_scope()

    def start_sources_selected(self) -> None:
        self.review_scope_combo.setCurrentIndex(self.review_scope_combo.findData("checked"))
        self.start_sources_for_scope()

    def start_sources_for_scope(self) -> None:
        if not self._ensure_review_job_can_start():
            return
        dataset_ids = self._dataset_ids_for_scope(self._review_scope())
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose dataset",
                "Check one or more datasets or switch scope to All datasets first.",
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
        self.review_scope_combo.setCurrentIndex(self.review_scope_combo.findData("all"))
        self.start_sources_for_scope()

    def apply_suggested_group(self) -> None:
        if self.repository is None:
            return
        dataset_id = self.selected_dataset_id()
        if not dataset_id:
            return
        self.repository.apply_suggested_group(dataset_id)
        self.refresh_all_views()

    def start_regroup_checked(self) -> None:
        self.grouping_scope_combo.setCurrentIndex(self.grouping_scope_combo.findData("checked"))
        self.start_regroup_for_scope()

    def start_regroup_for_scope(self) -> None:
        if not self._ensure_review_job_can_start():
            return
        scope = self._grouping_scope()
        dataset_ids = self._dataset_ids_for_scope(scope)
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets or switch scope to All datasets first.")
            return
        target_group_count = self._prompt_group_count(len(dataset_ids), "checked datasets" if scope == "checked" else "all datasets")
        if target_group_count <= 0:
            return
        self._start_worker(
            self._regroup_dataset_ids,
            dataset_ids,
            target_group_count,
            success_message="AI regrouping completed.",
            start_message=f"Regrouping {'checked datasets' if scope == 'checked' else 'all datasets'}...",
            activity_name="AI Regroup",
            refresh_after=True,
            track_review_job=True,
        )

    def start_regroup_all(self) -> None:
        self.grouping_scope_combo.setCurrentIndex(self.grouping_scope_combo.findData("all"))
        self.start_regroup_for_scope()

    def start_style_for_scope(self) -> None:
        if self.repository is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        dataset_ids = self._dataset_ids_for_scope(self._map_scope())
        if not dataset_ids:
            QMessageBox.information(
                self,
                "Choose datasets",
                "Mark one or more datasets as Visible on map, or switch scope to Show all first.",
            )
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
        group_id = self.selected_group_id()
        if not group_id:
            QMessageBox.information(self, "Choose group", "Select a group or a dataset within the group first.")
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
        self.repository.save_dataset_user_fields(
            dataset_id,
            display_name_user=self.dataset_name_edit.text().strip(),
            description_user=self.dataset_description_edit.toPlainText().strip(),
            visibility=self.visibility_checkbox.isChecked(),
            include_in_export=self.include_export_checkbox.isChecked(),
        )
        self.refresh_all_views()

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
        if self.repository is None:
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets first.")
            return
        self.repository.set_visibility_for_datasets(dataset_ids, True)
        self.map_scope_combo.setCurrentIndex(self.map_scope_combo.findData("visible"))
        self.append_activity_log(
            f"Enabled map visibility for {len(dataset_ids)} checked dataset(s).",
            activity="Selection Actions",
        )
        self.statusBar().showMessage(f"Enabled map visibility for {len(dataset_ids)} dataset(s).", 5000)
        self.refresh_all_views()

    def include_checked_in_report(self) -> None:
        if self.repository is None:
            return
        dataset_ids = self._checked_dataset_ids()
        if not dataset_ids:
            QMessageBox.information(self, "Choose datasets", "Check one or more datasets first.")
            return
        self.repository.set_include_in_export_for_datasets(dataset_ids, True)
        self.append_activity_log(
            f"Included {len(dataset_ids)} checked dataset(s) in report/export output.",
            activity="Selection Actions",
        )
        self.statusBar().showMessage(f"Included {len(dataset_ids)} dataset(s) in report/export.", 5000)
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
            return
        dataset_id = item.data(0, Qt.UserRole)
        if not dataset_id:
            return
        if item.checkState(0) == Qt.Checked:
            self._checked_dataset_id_set.add(str(dataset_id))
        else:
            self._checked_dataset_id_set.discard(str(dataset_id))

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
        return str(self.review_scope_combo.currentData() or "checked")

    def _grouping_scope(self) -> str:
        return str(self.grouping_scope_combo.currentData() or "checked")

    def _map_scope(self) -> str:
        return str(self.map_scope_combo.currentData() or "visible")

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
            f"Target number of groups for {scope_label}",
            default_value,
            1,
            max(1, dataset_count),
        )
        if not ok:
            return 0
        return int(value)

    def populate_inspector(self, dataset) -> None:
        if dataset is None:
            self.dataset_name_edit.clear()
            self.dataset_description_edit.clear()
            self.visibility_checkbox.setChecked(False)
            self.include_export_checkbox.setChecked(False)
            self.source_path_label.setText("-")
            self.geometry_label.setText("-")
            self.feature_count_label.setText("-")
            self.ai_title_label.setText("-")
            self.ai_group_label.setText("-")
            self.ai_description_box.setPlainText("")
            return
        self.dataset_name_edit.setText(dataset.display_name_user)
        self.dataset_description_edit.setPlainText(dataset.description_user)
        self.visibility_checkbox.setChecked(dataset.visibility)
        self.include_export_checkbox.setChecked(dataset.include_in_export)
        self.source_path_label.setText(dataset.source_path)
        self.geometry_label.setText(dataset.geometry_type)
        self.feature_count_label.setText(str(dataset.feature_count))
        self.ai_title_label.setText(dataset.display_name_ai or "-")
        self.ai_group_label.setText(dataset.suggested_group or "-")
        self.ai_description_box.setPlainText(dataset.description_ai or "")

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

    def export_geoparquet(self) -> None:
        if self.export_service is None or self.current_workspace is None:
            QMessageBox.information(self, "No project", "Load or scan a folder first.")
            return
        default_path = self.current_workspace.exports_dir / DEFAULT_EXPORT_PARQUET_NAME
        path, _ = QFileDialog.getSaveFileName(self, "Export GeoParquet", str(default_path), "Parquet (*.parquet)")
        if not path:
            return
        self._start_worker(
            self.export_service.export_geoparquet,
            path,
            success_message="GeoParquet export completed.",
            start_message="Exporting GeoParquet...",
            activity_name="Export GeoParquet",
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
            style = self.style_service.style_for_dataset(
                dataset,
                group_name=groups.get(dataset.group_id, dataset.group_id),
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
        self.refresh_map()

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
                SortableTableWidgetItem(dataset.source_path, dataset.source_path.lower()),
            )
        self.import_table.setSortingEnabled(True)
        if sort_section >= 0:
            self.import_table.sortItems(sort_section, sort_order)
        if self.repository is None:
            self.import_summary.setText("No folder loaded.")
        else:
            summary = self.repository.summary()
            self.import_summary.setText(
                f"Datasets: {summary['dataset_count']} | Groups: {summary['group_count']} | "
                f"Source candidates: {summary['source_count']} | Styles: {summary['style_count']}"
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
                    child.setToolTip(0, "Checked = selected for batch actions in Review")
                    group_item.addChild(child)
                    group_dataset_ids.append(dataset.dataset_id)
                    if dataset.dataset_id == selected_id:
                        self.tree.setCurrentItem(child)
                group_item.setCheckState(0, self._group_check_state(group_dataset_ids))
                group_item.setToolTip(0, "Checked group = all datasets in the group are selected for batch actions")
                group_item.setExpanded(True)
        finally:
            self._populating_tree = False

    def refresh_map(self) -> None:
        datasets = self._datasets()
        style_count = self.repository.summary()["style_count"] if self.repository is not None else 0
        map_scope = self._map_scope()
        map_dataset_ids = self._dataset_ids_for_scope(map_scope) if self.repository is not None else []
        self._map_refresh_pending = True
        if self.repository is None:
            self.map_summary.setText("No project loaded.")
            return
        if self.map_bridge is not None:
            self.map_bridge.set_scope(map_scope)
        if self._review_job_running:
            self.map_summary.setText(
                f"Map layers available: {len(map_dataset_ids)} of {len(datasets)} | Styled: {style_count}. "
                "Map loading is paused while dataset processing is running."
            )
            return
        if not self._is_map_tab_active():
            self.map_summary.setText(
                f"Map layers available: {len(map_dataset_ids)} of {len(datasets)} | Styled: {style_count}. "
                "Open the Map / Export tab to load the map in browse mode."
            )
            return
        self._ensure_map_ready()
        if self.map_bridge is not None:
            self.map_bridge.set_scope(map_scope)
        scope_note = "using datasets marked Visible on map." if map_scope == "visible" else "showing all datasets."
        self.map_summary.setText(
            f"Map layers available: {len(map_dataset_ids)} of {len(datasets)} | Styled: {style_count}. "
            f"Browse mode loads one layer at a time by default, {scope_note}"
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
        self._map_refresh_pending = True
        self._load_activity_log()
        self._update_folder_actions()

    def _rebuild_ai_services(self) -> None:
        classifier = OpenAIClassificationProvider(
            api_key=self.current_settings.openai_api_key or None,
            model=self.current_settings.openai_model or DEFAULT_OPENAI_MODEL,
            endpoint=self.current_settings.openai_endpoint,
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
            f"Manual AI context: {context_summary} | "
            f"Live search timeout: {self.current_settings.search_timeout_s:g}s | "
            f"Search failover: {self.current_settings.search_max_consecutive_failures}"
        )

    def _apply_settings_to_form(self, settings: AppSettings) -> None:
        self.settings_model_combo.setCurrentText(settings.openai_model)
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

    def _setup_map_bridge(self) -> None:
        if not WEBENGINE_AVAILABLE or self.current_workspace is None or self.repository is None:
            return
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
            for attribute_name in ("LocalContentCanAccessRemoteUrls", "LocalContentCanAccessFileUrls"):
                attribute = getattr(QWebEngineSettings, attribute_name, None)
                if attribute is not None:
                    settings.setAttribute(attribute, True)
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
        if not self._is_map_tab_active():
            return
        self.refresh_map()
        if self._map_initialized and WEBENGINE_AVAILABLE and hasattr(self.map_view, "page"):
            try:
                self.map_view.page().runJavaScript("window.dispatchEvent(new Event('resize'));")
            except Exception:
                pass

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
        worker.signals.finished.connect(lambda token=progress_token, message=success_message: self._finish_background_activity(token, message))
        worker.signals.finished.connect(lambda: self.statusBar().showMessage(success_message, 5000))
        if track_review_job:
            worker.signals.finished.connect(lambda: self._on_review_job_finished(success_message))
        if refresh_after:
            worker.signals.result.connect(lambda _value, activity=log_activity: self._schedule_refresh_all_views_after_worker(activity))
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
            auto_assign_group=True,
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
            if dataset.group_id in {"", "ungrouped"} and understanding.suggested_group:
                self.repository.assign_group(dataset_id, understanding.suggested_group)
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

    def _regroup_dataset_ids(
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
        refreshed_hint_count = 0
        total = len(dataset_ids)
        if progress_callback:
            progress_callback(0)
        if status_callback:
            status_callback(
                f"Regroup has a total time budget of {self._format_elapsed_seconds(REGROUP_TOTAL_TIMEOUT_S)}. "
                f"Preparing grouping hints for {total} dataset(s). "
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
            needs_hint_refresh = not dataset.display_name_ai and not dataset.description_ai and not dataset.suggested_group
            if needs_hint_refresh:
                if status_callback:
                    status_callback(f"Preparing grouping hints {index}/{total}: {dataset.preferred_name}")
                understanding = self.heuristic_intelligence_service.classify(dataset)
                self.repository.upsert_understanding(dataset_id, understanding)
                dataset = self.repository.get_dataset(dataset_id) or dataset
                refreshed_hint_count += 1
            datasets.append(dataset)
            prepared_dataset_ids.add(dataset.dataset_id)
            if progress_callback:
                progress_callback(int((index / max(total, 1)) * 35))

        if status_callback and datasets:
            reused_count = max(0, len(datasets) - refreshed_hint_count)
            status_callback(
                f"Grouping hints ready for {len(datasets)} dataset(s): "
                f"{refreshed_hint_count} refreshed, {reused_count} reused."
            )

        if not datasets and not timed_out_dataset_ids:
            return 0

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
                raw_assignments = self._group_datasets_with_timeout(
                    datasets,
                    target_group_count,
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

        if not assignments:
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

    def on_background_error(self, message: str, progress_token: int | None = None) -> None:
        self.append_activity_log(message)
        self.review_job_status.setText("Background job failed.")
        self._review_job_running = False
        if progress_token is not None:
            self._finish_background_activity(progress_token, "Background job failed.")
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
        self._active_background_progress_token = 0
        self._background_activity_progress_value = None
        self._background_activity_started_at = None
        self._background_activity_worker_signal_at = None
        self._background_heartbeat_timer.stop()
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
        assets_dir = Path(__file__).resolve().parent / "assets"
        checked_icon = (assets_dir / "checkmark_checked.svg").as_posix()
        indeterminate_icon = (assets_dir / "checkmark_indeterminate.svg").as_posix()
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
            QWidget#ImportTab, QWidget#ReviewTab, QWidget#MapTab, QWidget#SettingsTab, QWidget#AboutTab {
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
            QSplitter::handle {
                background-color: #d9ccb6;
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

