from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from grasp.branding import APP_AUTHOR, APP_LINKEDIN_URL, APP_REPOSITORY_URL, APP_TAGLINE, APP_WINDOW_TITLE
from grasp.intelligence.providers import HeuristicClassificationProvider, OpenAIClassificationProvider
from grasp.intelligence.service import IntelligenceService
from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate
from grasp.qt_compat import QApplication, QAbstractItemView, QDialog, QGridLayout, QHBoxLayout, QLabel, QMessageBox, QPlainTextEdit, Qt, QVBoxLayout
from grasp.ui.main_window import (
    ABOUT_ILLUSTRATION_PATH,
    APP_ICON_PATH,
    COMPACT_ACTION_BUTTON_HEIGHT_PX,
    COMPACT_ACTION_BUTTON_WIDTH_PX,
    MANAGE_ACTION_BUTTON_HEIGHT_PX,
    MANAGE_ACTION_BUTTON_WIDTH_PX,
    MAP_HTTP_USER_AGENT,
    TAB_PAGE_MARGIN_PX,
    MainWindow,
)


class MainWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

    def test_webengine_view_is_not_constructed_until_map_surface_is_needed(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", True), patch("grasp.ui.main_window.LoggingWebEnginePage", None):
            created: list[str] = []

            class _WebEngineView(QPlainTextEdit):
                def __init__(self) -> None:
                    super().__init__()
                    created.append("created")

                def page(self):
                    return object()

            with patch("grasp.ui.main_window.QWebEngineView", _WebEngineView):
                window = MainWindow()
                try:
                    self.assertEqual(created, [])
                    self.assertIn("initialize the embedded map preview", window.map_view.toPlainText())

                    self.assertTrue(window._ensure_webengine_map_view())

                    self.assertEqual(created, ["created"])
                    self.assertIsInstance(window.map_view, _WebEngineView)
                finally:
                    window.close()

    def test_map_loading_is_deferred_until_map_tab_is_opened(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertEqual(window.windowTitle(), APP_WINDOW_TITLE)
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()

                    self.assertFalse(window._map_initialized)
                    self.assertTrue(window._map_refresh_pending)
                    self.assertIn("Open the Map tab", window.map_summary.text())
                    self.assertIn("Checked working set:", window.map_summary.text())
                    self.assertIn("current scope (Visible on map)", window.map_summary.text())

                    window.tabs.setCurrentWidget(window.map_tab)

                    self.assertTrue(window._map_initialized)
                    self.assertFalse(window._map_refresh_pending)
            finally:
                window.close()

    def test_load_existing_and_reset_buttons_follow_catalog_presence(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window.folder_edit.setText(tmp)
                    self.assertFalse(window.load_catalog_button.isEnabled())
                    self.assertFalse(window.reset_all_data_button.isEnabled())

                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )
                    window._update_folder_actions()

                    self.assertTrue(window.load_catalog_button.isEnabled())
                    self.assertTrue(window.reset_all_data_button.isEnabled())
            finally:
                window.close()

    def test_use_ai_for_selected_dataset_copies_ai_fields_into_editable_fields(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                display_name_ai="AI Roads",
                                description_ai="AI generated description",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    child_item = group_item.child(0)
                    window.tree.setCurrentItem(child_item)
                    window.use_ai_for_selected_dataset()

                    stored = window.repository.get_dataset("ds1")
                    self.assertEqual(stored.display_name_user, "AI Roads")
                    self.assertEqual(stored.description_user, "AI generated description")
                    self.assertEqual(window.dataset_name_edit.text(), "AI Roads")
                    self.assertEqual(window.dataset_description_edit.toPlainText(), "AI generated description")
                    self.assertIn("The checked working set is shared with the checkboxes in the dataset list on this page", window.review_visibility_note.text())
            finally:
                window.close()

    def test_dataset_controls_live_inside_datasets_group_box(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertEqual(
                    [window.tabs.tabText(index) for index in range(window.tabs.count())],
                    ["Import", "Manage data", "Review datasets", "Map", "Settings", "About"],
                )
                self.assertTrue(window.tabs.tabBar().usesScrollButtons())
                self.assertEqual(window.tabs.tabBar().elideMode(), Qt.ElideRight)
                self.assertFalse(window.tabs.tabBar().expanding())
                self.assertIsInstance(window.import_tab.layout().itemAt(0).layout(), QGridLayout)
                self.assertEqual(window.import_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.review_datasets_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.info_sources_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.map_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.settings_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.about_tab.layout().contentsMargins().left(), TAB_PAGE_MARGIN_PX)
                self.assertEqual(window.datasets_group_box.title(), "Datasets")
                self.assertEqual(window.browse_button.text(), "Browse")
                self.assertEqual(window.rebuild_archive_button.text(), "Rebuild archive")
                self.assertEqual(window.load_catalog_button.text(), "Load from database")
                self.assertEqual(window.reset_all_data_button.text(), "Delete database")
                self.assertEqual(window.simplify_import_names_checkbox.text(), "Simplify long dataset names on import")
                self.assertIn("Description", window.simplify_import_names_checkbox.toolTip())
                self.assertEqual(window.collect_available_metadata_checkbox.text(), "Collect available metadata")
                self.assertIn(".xml", window.collect_available_metadata_checkbox.toolTip())
                self.assertEqual(window.selection_group_box.title(), "1. Choose datasets for batch work")
                self.assertEqual(window.grouping_group_box.title(), "2. Groups")
                self.assertEqual(window.dataset_actions_group_box.title(), "3. Apply batch changes")
                self.assertEqual(window.dataset_details_group_box.title(), "Selected dataset")
                self.assertEqual(window.ai_settings_group_box.title(), "AI Settings")
                self.assertEqual(window.search_settings_group_box.title(), "Search Settings")
                self.assertEqual(window.ai_context_group_box.title(), "AI Classification Context")
                self.assertEqual(window.select_all_button.text(), "Select All")
                self.assertEqual(window.clear_all_button.text(), "Clear All")
                self.assertEqual(window.add_group_button.text(), "Add Group")
                self.assertEqual(window.clear_group_button.text(), "Clear Group")
                self.assertEqual(window.regroup_button.text(), "AI Regroup...")
                self.assertEqual(window.reset_groups_button.text(), "Reset Groups")
                self.assertEqual(window.generate_styles_button.text(), "Generate Styles")
                self.assertEqual(window.transfer_ai_selected_button.text(), "Transfer AI to Name + Description")
                self.assertEqual(window.save_dataset_button.text(), "Save Changes")
                self.assertEqual(window.fill_ai_fields_button.text(), "Fill Empty Fields from AI")
                self.assertEqual(window.make_visible_button.text(), "Make visible in maps")
                self.assertEqual(window.hide_from_maps_button.text(), "Hide from maps")
                self.assertEqual(window.include_in_report_button.text(), "Include in export")
                self.assertEqual(window.exclude_from_report_button.text(), "Exclude from export")
                self.assertEqual(window.select_all_button.minimumHeight(), COMPACT_ACTION_BUTTON_HEIGHT_PX)
                self.assertEqual(window.export_gpkg_button.minimumHeight(), MANAGE_ACTION_BUTTON_HEIGHT_PX)
                self.assertEqual(window.export_gpkg_button.minimumWidth(), MANAGE_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.select_all_button.minimumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.clear_all_button.minimumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.add_group_button.minimumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.clear_group_button.minimumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.select_all_button.maximumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.clear_all_button.maximumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.add_group_button.maximumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.clear_group_button.maximumWidth(), COMPACT_ACTION_BUTTON_WIDTH_PX)
                self.assertEqual(window.selection_group_combo.maximumWidth(), 320)
                self.assertEqual(window.transfer_ai_selected_button.maximumWidth(), 220)
                self.assertEqual(window.save_dataset_button.maximumWidth(), 150)
                self.assertEqual(window.review_dataset_filter_edit.placeholderText(), "Filter datasets by name, group, format, geometry or source path")
                self.assertEqual(window.dataset_nav_first_button.text(), "First")
                self.assertEqual(window.dataset_nav_back_button.text(), "Back")
                self.assertEqual(window.dataset_nav_next_button.text(), "Next")
                self.assertEqual(window.dataset_nav_last_button.text(), "Last")
                self.assertIn("define one checked working set", window.info_sources_intro_label.text().lower())
                self.assertIn("drives steps 2 and 3", window.selection_help_label.text().lower())
                self.assertIn("does not control the map tab", window.selection_help_label.text().lower())
                self.assertTrue(window.review_datasets_tab.isAncestorOf(window.review_dataset_filter_edit))
                self.assertTrue(window.review_datasets_tab.isAncestorOf(window.review_dataset_table))
                self.assertTrue(window.review_datasets_tab.isAncestorOf(window.dataset_details_group_box))
                self.assertTrue(window.review_datasets_tab.isAncestorOf(window.dataset_nav_first_button))
                self.assertEqual(window.review_dataset_splitter.orientation(), Qt.Horizontal)
                self.assertFalse(window.review_dataset_splitter.childrenCollapsible())
                self.assertEqual(window.review_dataset_splitter.handleWidth(), 12)
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.review_progress))
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.review_job_status))
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.export_gpkg_button))
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.generate_styles_button))
                self.assertFalse(window.map_tab.isAncestorOf(window.export_gpkg_button))
                self.assertFalse(window.map_tab.isAncestorOf(window.generate_styles_button))
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.selection_group_box))
                self.assertIs(window.info_sources_tab.layout().itemAt(1).widget(), window.selection_group_box)
                self.assertIs(window.info_sources_tab.layout().itemAt(2).widget(), window.datasets_group_box)
                self.assertIsInstance(window.info_sources_tab.layout().itemAt(3).layout(), QHBoxLayout)
                self.assertIs(window.info_sources_tab.layout().itemAt(4).widget(), window.review_job_group_box)
                self.assertTrue(window.review_job_group_box.isAncestorOf(window.review_progress))
                self.assertTrue(window.review_job_group_box.isAncestorOf(window.review_visibility_note))
                self.assertTrue(window.info_sources_tab.isAncestorOf(window.datasets_group_box))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.tree))
                self.assertFalse(hasattr(window, "review_actions_group_box"))
                self.assertFalse(hasattr(window, "find_info_fast_button"))
                self.assertFalse(hasattr(window, "find_info_ai_button"))
                self.assertFalse(hasattr(window, "find_sources_button"))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.selection_group_combo))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.select_all_button))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.clear_all_button))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.add_group_button))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.clear_group_button))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.selection_help_label))
                self.assertTrue(window.selection_group_box.isAncestorOf(window.selection_scope_status_label))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.new_group_button))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.apply_group_button))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.regroup_button))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.reset_groups_button))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.grouping_help_label))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.fill_ai_fields_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.make_visible_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.hide_from_maps_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.include_in_report_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.generate_styles_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.exclude_from_report_button))
                self.assertTrue(window.dataset_actions_group_box.isAncestorOf(window.dataset_actions_help_label))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.dataset_name_edit))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.dataset_group_combo))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.source_style_label))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.ai_description_box))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.raw_import_data_box))
                self.assertTrue(window.dataset_details_group_box.isAncestorOf(window.save_dataset_button))
                self.assertIsInstance(window.map_tab.layout().itemAt(0).layout(), QHBoxLayout)
                self.assertEqual(window.review_dataset_table.editTriggers(), QAbstractItemView.NoEditTriggers)
                self.assertEqual(window.review_dataset_table.selectionBehavior(), QAbstractItemView.SelectRows)
                self.assertEqual(window.review_dataset_table.selectionMode(), QAbstractItemView.SingleSelection)
                self.assertFalse(window.review_dataset_table.isSortingEnabled())
                group_box_titles = [group_box.title() for group_box in window.findChildren(type(window.datasets_group_box))]
                self.assertNotIn("Source Candidates", group_box_titles)
                self.assertFalse(hasattr(window, "sources_table"))
                self.assertEqual(window.dataset_description_edit.minimumHeight(), 72)
                self.assertEqual(window.dataset_description_edit.maximumHeight(), 96)
                self.assertEqual(window.ai_description_box.minimumHeight(), 72)
                self.assertEqual(window.ai_description_box.maximumHeight(), 96)
                self.assertEqual(window.raw_import_data_box.minimumHeight(), 96)
                self.assertEqual(window.raw_import_data_box.maximumHeight(), 140)
                self.assertTrue(window.raw_import_data_box.isReadOnly())
                self.assertTrue(hasattr(window, "settings_search_timeout_edit"))
                self.assertTrue(hasattr(window, "settings_search_failures_edit"))
                self.assertTrue(hasattr(window, "settings_search_candidates_edit"))
                self.assertTrue(hasattr(window, "settings_data_language_combo"))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.settings_model_combo))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.settings_data_language_combo))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.settings_timeout_edit))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.ai_context_group_box))
                self.assertTrue(hasattr(window, "settings_columns_layout"))
                self.assertIs(window.settings_columns_layout.itemAt(0).widget(), window.ai_settings_group_box)
                self.assertIs(window.settings_columns_layout.itemAt(1).widget(), window.search_settings_group_box)
                self.assertEqual(window.settings_data_language_combo.itemText(0), "Not set")
                self.assertTrue(window.ai_context_group_box.isAncestorOf(window.settings_context_column_names_checkbox))
                self.assertTrue(window.search_settings_group_box.isAncestorOf(window.settings_search_timeout_edit))
                self.assertTrue(window.search_settings_group_box.isAncestorOf(window.settings_search_candidates_edit))
                self.assertTrue(hasattr(window, "log_button"))
                self.assertTrue(hasattr(window, "exit_button"))
                self.assertEqual(window.log_button.minimumWidth(), 82)
                self.assertEqual(window.log_button.maximumWidth(), 104)
                self.assertEqual(window.exit_button.minimumWidth(), 58)
                self.assertEqual(window.exit_button.maximumWidth(), 72)
                self.assertEqual(window.exit_button.objectName(), "CornerExitButton")
                self.assertTrue(hasattr(window, "map_scope_combo"))
                self.assertTrue(hasattr(window, "refresh_map_button"))
                self.assertTrue(hasattr(window, "map_scope_label"))
                self.assertEqual(window.map_scope_combo.itemText(0), "Visible on map")
                self.assertEqual(window.map_scope_combo.itemData(0), "visible")
                self.assertEqual(window.map_scope_combo.itemText(1), "Checked working set")
                self.assertEqual(window.map_scope_combo.itemData(1), "checked")
                self.assertEqual(window.map_scope_combo.itemText(2), "Show all")
                self.assertEqual(window.map_scope_combo.itemData(2), "all")
                self.assertEqual(window._map_scope(), "visible")
                self.assertIs(window.map_controls_layout.itemAt(0).widget(), window.refresh_map_button)
                self.assertIs(window.map_controls_layout.itemAt(2).widget(), window.map_scope_label)
                self.assertIs(window.map_controls_layout.itemAt(3).widget(), window.map_scope_combo)
                self.assertTrue(window.import_table.isSortingEnabled())
                self.assertEqual(window.import_table.editTriggers(), QAbstractItemView.NoEditTriggers)
                self.assertIn("QTreeWidget::item:selected", window.styleSheet())
                self.assertIn("show-decoration-selected: 1;", window.styleSheet())
                self.assertIn("QTreeWidget::indicator:checked", window.styleSheet())
                self.assertIn("background-color: #9a7230;", window.styleSheet())
                self.assertIn("checkmark_checked.svg", window.styleSheet())
                self.assertIn("checkmark_indeterminate.svg", window.styleSheet())
                self.assertIn("QPushButton#CornerExitButton", window.styleSheet())
                self.assertIn("max-height: 24px;", window.styleSheet())
            finally:
                window.close()

    def test_review_action_boxes_use_left_to_right_grid_layouts(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                selection_layout = window.selection_group_box.layout()
                grouping_layout = window.grouping_group_box.layout()
                dataset_actions_layout = window.dataset_actions_group_box.layout()

                self.assertIsInstance(selection_layout, QVBoxLayout)
                self.assertIsInstance(grouping_layout, QGridLayout)
                self.assertIsInstance(dataset_actions_layout, QGridLayout)

                selection_controls_layout = selection_layout.itemAt(1).layout()
                self.assertIsInstance(selection_controls_layout, QHBoxLayout)
                self.assertIs(selection_controls_layout.itemAt(0).widget(), window.select_all_button)
                self.assertIs(selection_controls_layout.itemAt(1).widget(), window.clear_all_button)
                self.assertIs(selection_controls_layout.itemAt(2).widget(), window.add_group_button)
                self.assertIs(selection_controls_layout.itemAt(3).widget(), window.clear_group_button)
                self.assertIs(selection_controls_layout.itemAt(6).widget(), window.selection_group_combo)

                self.assertEqual(grouping_layout.getItemPosition(grouping_layout.indexOf(window.new_group_button))[:2], (0, 0))
                self.assertEqual(grouping_layout.getItemPosition(grouping_layout.indexOf(window.apply_group_button))[:2], (0, 1))
                self.assertEqual(grouping_layout.getItemPosition(grouping_layout.indexOf(window.regroup_button))[:2], (1, 0))
                self.assertEqual(grouping_layout.getItemPosition(grouping_layout.indexOf(window.reset_groups_button))[:2], (1, 1))
                self.assertEqual(grouping_layout.getItemPosition(grouping_layout.indexOf(window.grouping_help_label))[:2], (2, 0))

                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.fill_ai_fields_button))[:2], (0, 0))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.make_visible_button))[:2], (0, 1))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.hide_from_maps_button))[:2], (1, 0))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.include_in_report_button))[:2], (1, 1))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.generate_styles_button))[:2], (2, 0))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.exclude_from_report_button))[:2], (2, 1))
                self.assertEqual(dataset_actions_layout.getItemPosition(dataset_actions_layout.indexOf(window.export_gpkg_button))[:2], (3, 0))
            finally:
                window.close()

    def test_review_dataset_browser_filters_and_navigates_selected_dataset(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="alpha",
                                source_path="D:/data/alpha.geojson",
                                source_format="geojson",
                                layer_name="Alpha",
                                cache_path="alpha.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="bravo",
                                source_path="D:/data/bravo.geojson",
                                source_format="geojson",
                                layer_name="Bravo",
                                cache_path="bravo.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="charlie",
                                source_path="D:/data/charlie.gpkg",
                                source_format="gpkg",
                                layer_name="Charlie",
                                cache_path="charlie.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()

                    self.assertEqual(window.review_dataset_table.rowCount(), 3)

                    window.review_dataset_table.selectRow(1)
                    self.assertEqual(window.selected_dataset_id(), "bravo")
                    self.assertEqual(window.dataset_name_edit.text(), "Bravo")

                    window.select_next_review_dataset()
                    self.assertEqual(window.selected_dataset_id(), "charlie")
                    self.assertEqual(window.dataset_name_edit.text(), "Charlie")

                    window.select_first_review_dataset()
                    self.assertEqual(window.selected_dataset_id(), "alpha")
                    self.assertEqual(window.dataset_name_edit.text(), "Alpha")

                    window.select_last_review_dataset()
                    self.assertEqual(window.selected_dataset_id(), "charlie")

                    window.select_previous_review_dataset()
                    self.assertEqual(window.selected_dataset_id(), "bravo")

                    window.review_dataset_filter_edit.setText("gpkg")
                    self.assertEqual(window.review_dataset_table.rowCount(), 1)
                    self.assertEqual(window.review_dataset_table.item(0, 0).text(), "Charlie")
                    self.assertEqual(window.review_dataset_table.item(0, 1).text(), "Ungrouped")
                    self.assertEqual(window.review_dataset_table.item(0, 2).text(), "gpkg")
            finally:
                window.close()

    def test_review_dataset_group_combo_reassigns_selected_dataset(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Transport")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                group_id="ungrouped",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    window._set_review_dataset_table_selection("b")

                    self.assertEqual(window.selected_dataset_id(), "b")
                    self.assertEqual(window.dataset_group_combo.currentData(), "ungrouped")

                    transport_index = window.dataset_group_combo.findData("transport")
                    self.assertGreaterEqual(transport_index, 0)

                    window.dataset_group_combo.setCurrentIndex(transport_index)

                    self.assertEqual(window.repository.get_dataset("b").group_id, "transport")
                    self.assertEqual(window.dataset_group_combo.currentData(), "transport")
                    self.assertIn("Transport", [window.dataset_group_combo.itemText(index) for index in range(window.dataset_group_combo.count())])
                    row_index = window._dataset_browser_row_ids.index("b")
                    self.assertEqual(window.review_dataset_table.item(row_index, 1).text(), "Transport")
            finally:
                window.close()

    def test_group_combos_sort_choices_alphabetically(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Zebra")
                    window.repository.create_group("Alpha")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                group_id="ungrouped",
                                cache_path="a.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    window._set_review_dataset_table_selection("a")

                    selection_group_names = [
                        window.selection_group_combo.itemText(index) for index in range(window.selection_group_combo.count())
                    ]
                    dataset_group_names = [
                        window.dataset_group_combo.itemText(index) for index in range(window.dataset_group_combo.count())
                    ]

                    self.assertEqual(selection_group_names, ["Alpha", "Ungrouped", "Zebra"])
                    self.assertEqual(dataset_group_names, ["Alpha", "Ungrouped", "Zebra"])
            finally:
                window.close()

    def test_populate_inspector_shows_source_style_summary(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="styled",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                raw_import_data="<metadata><citation>Road dataset</citation></metadata>",
                                source_style_summary="Possible source styling detected: QGIS QML style file (roads.qml).",
                                source_style_items_json='[{"kind":"sidecar:qml","label":"QGIS QML style file (roads.qml)","path":"D:/data/roads.qml"}]',
                                cache_path="styled.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    child_item = group_item.child(0)
                    window.tree.setCurrentItem(child_item)

                    self.assertIn("roads.qml", window.source_style_label.text())
                    self.assertIn("roads.qml", window.source_style_label.toolTip())
                    self.assertIn("Road dataset", window.raw_import_data_box.toPlainText())
            finally:
                window.close()

    def test_import_table_supports_sorting_without_editing(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/zones.geojson",
                                source_format="geojson",
                                layer_name="Zones",
                                geometry_type="Polygon",
                                feature_count=12,
                                cache_path="b.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/roads.geojson",
                                source_format="shp",
                                layer_name="Roads",
                                geometry_type="LineString",
                                feature_count=2,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="c",
                                source_path="D:/data/places.parquet",
                                source_format="parquet",
                                layer_name="Places",
                                geometry_type="Point",
                                feature_count=30,
                                cache_path="c.parquet",
                            ),
                        ]
                    )

                    window.refresh_import_table()
                    window.import_table.sortItems(3, Qt.AscendingOrder)

                    self.assertEqual(window.import_table.item(0, 0).text(), "Roads")
                    self.assertEqual(window.import_table.item(1, 0).text(), "Zones")
                    self.assertEqual(window.import_table.item(2, 0).text(), "Places")
                    self.assertEqual(window.import_table.editTriggers(), QAbstractItemView.NoEditTriggers)
            finally:
                window.close()

    def test_import_table_marks_possible_source_styling(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="styled",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                source_style_summary="Possible source styling detected: QGIS QML style file (roads.qml).",
                                source_style_items_json='[{"kind":"sidecar:qml","label":"QGIS QML style file (roads.qml)","path":"D:/data/roads.qml"}]',
                                cache_path="styled.parquet",
                            )
                        ]
                    )

                    window.refresh_import_table()

                    self.assertEqual(window.import_table.item(0, 4).text(), "Possible styling")
                    self.assertIn("Possible source styling: 1", window.import_summary.text())
            finally:
                window.close()

    def test_setup_map_bridge_configures_web_profile_user_agent_and_cache(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    user_agents: list[str] = []
                    cache_paths: list[str] = []
                    storage_paths: list[str] = []
                    web_channels: list[object] = []
                    loaded_urls: list[str] = []

                    class _Settings:
                        def __init__(self) -> None:
                            self.attributes = []

                        def setAttribute(self, attribute, value) -> None:
                            self.attributes.append((attribute, value))

                    class _Profile:
                        def setHttpUserAgent(self, value: str) -> None:
                            user_agents.append(value)

                        def setCachePath(self, value: str) -> None:
                            cache_paths.append(value)

                        def setPersistentStoragePath(self, value: str) -> None:
                            storage_paths.append(value)

                    class _Page:
                        def __init__(self) -> None:
                            self._settings = _Settings()
                            self._profile = _Profile()

                        def settings(self):
                            return self._settings

                        def profile(self):
                            return self._profile

                        def setWebChannel(self, channel) -> None:
                            web_channels.append(channel)

                    class _MapView:
                        def __init__(self) -> None:
                            self._page = _Page()

                        def page(self):
                            return self._page

                        def load(self, url) -> None:
                            loaded_urls.append(url.toString())

                    class _Channel:
                        def __init__(self, _page) -> None:
                            self.registered = []

                        def registerObject(self, name, value) -> None:
                            self.registered.append((name, value))

                    window.map_view = _MapView()

                    with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", True), patch(
                        "grasp.ui.main_window.QWebChannel",
                        _Channel,
                    ):
                        window._setup_map_bridge()

                    self.assertEqual(user_agents, [MAP_HTTP_USER_AGENT])
                    self.assertEqual(len(cache_paths), 1)
                    self.assertEqual(len(storage_paths), 1)
                    self.assertTrue(cache_paths[0].endswith("data_out\\web_cache") or cache_paths[0].endswith("data_out/web_cache"))
                    self.assertTrue(storage_paths[0].endswith("data_out\\web_profile") or storage_paths[0].endswith("data_out/web_profile"))
                    self.assertEqual(len(web_channels), 1)
                    self.assertEqual(len(loaded_urls), 1)
            finally:
                window.close()

    def test_review_ui_stays_stable_when_sources_exist_in_repository(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )
                    window.repository.replace_sources(
                        "ds1",
                        [
                            SourceCandidate(
                                url="https://example.org/roads",
                                title="Official roads",
                                domain="example.org",
                                source_type="official",
                                confidence=0.91,
                                candidate_id="src1",
                            ),
                            SourceCandidate(
                                url="https://mirror.example.org/roads",
                                title="Mirror roads",
                                domain="mirror.example.org",
                                source_type="search-result",
                                confidence=0.67,
                                candidate_id="src2",
                            ),
                        ],
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    child_item = group_item.child(0)
                    window.tree.setCurrentItem(child_item)

                    self.assertEqual(window.dataset_name_edit.text(), "Roads")
                    self.assertEqual(window.ai_title_label.text(), "-")
                    self.assertFalse(hasattr(window, "sources_table"))
            finally:
                window.close()

    def test_about_tab_describes_grasp_and_author(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertEqual(window.about_header_label.text(), "GRASP")
                self.assertIn("stands for", window.about_acronym_label.text())
                self.assertIn(APP_TAGLINE, window.about_acronym_label.text())
                self.assertIn(APP_TAGLINE, window.about_tagline_label.text())
                self.assertIn("untidy early stage of geospatial work", window.about_tagline_label.text())
                self.assertIn(APP_AUTHOR, window.about_author_label.text())
                self.assertEqual(window.about_illustration_path, ABOUT_ILLUSTRATION_PATH)
                self.assertEqual(window.about_icon_path, APP_ICON_PATH)
                self.assertTrue(window.about_illustration_path.exists())
                self.assertTrue(window.about_icon_path.exists())
                self.assertIn("Why it exists:", window.about_purpose_label.text())
                self.assertIn("different teams, vintages, and file formats", window.about_purpose_label.text())
                self.assertIn("In practice", window.about_mission_label.text())
                self.assertIn("exports a packaged GeoPackage", window.about_capabilities_label.text())
                self.assertIn("QGIS project support", window.about_capabilities_label.text())
                self.assertIn("speed, consistency", window.about_note_label.text())
                self.assertIn("not replace judgment", window.about_note_label.text())
                self.assertIn(APP_LINKEDIN_URL, window.about_links_label.text())
                self.assertIn(APP_REPOSITORY_URL, window.about_links_label.text())
                self.assertTrue(window.about_links_label.openExternalLinks())
                self.assertEqual(window.about_illustration_label.objectName(), "AboutIllustration")
                self.assertTrue(window.about_body_host.layout() is window.about_body_layout)
                self.assertTrue(window.about_text_panel.isAncestorOf(window.about_purpose_label))
            finally:
                window.close()

    def test_log_button_reflects_global_background_activity(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                token = window._begin_background_activity("Working...", activity="Test Job")
                self.assertEqual(window.log_button.text(), "Logs*")
                self.assertIn("[Test Job] - starting", window.log_text.toPlainText())

                window._update_background_activity_progress(token, 55)
                self.assertEqual(window.log_button.text(), "Logs* 55%")

                window._background_activity_started_at = 0.0
                with patch("grasp.ui.main_window.monotonic", return_value=75.0):
                    window._emit_background_activity_heartbeat()
                self.assertIn("[Test Job] - still running (01:15 elapsed)", window.log_text.toPlainText())

                window._finish_background_activity(token, "Done.")
                self.assertEqual(window.log_button.text(), "Logs")
                self.assertIn("[Test Job] - ending", window.log_text.toPlainText())
            finally:
                window.close()

    def test_background_heartbeat_includes_latest_status_step(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                token = window._begin_background_activity("Working...", activity="AI Regroup")
                window._update_background_activity_status(token, "Waiting for grouping response.")
                window._background_activity_started_at = 0.0

                with patch("grasp.ui.main_window.monotonic", return_value=75.0):
                    window._emit_background_activity_heartbeat()

                self.assertIn(
                    "[AI Regroup] - still running (01:15 elapsed); latest step: Waiting for grouping response.",
                    window.log_text.toPlainText(),
                )
            finally:
                window.close()

    def test_stale_review_job_lock_is_cleared_after_long_idle_period(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                token = window._begin_background_activity("Working...", activity="AI Regroup")
                window._review_job_running = True
                window._active_background_progress_token = token
                window._background_activity_worker_signal_at = 0.0

                with patch("grasp.ui.main_window.monotonic", return_value=601.0):
                    cleared = window._clear_stale_review_job_lock_if_needed()

                self.assertTrue(cleared)
                self.assertFalse(window._review_job_running)
                self.assertEqual(window._active_background_progress_token, 0)
                self.assertEqual(window.log_button.text(), "Logs")
                self.assertIn("Cleared a stale review-job lock after 10:01 without worker updates.", window.log_text.toPlainText())
            finally:
                window.close()

    def test_global_log_button_opens_global_log_window(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.append_activity_log("First message", activity="Manual")
                window.open_log_window()
                self.app.processEvents()

                self.assertTrue(window.log_window.isVisible())
                self.assertIn("First message", window.log_text.toPlainText())
            finally:
                window.log_window.close()
                window.close()

    def test_review_dataset_splitter_tracks_window_width(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.resize(900, 700)
                window.show()
                window.tabs.setCurrentWidget(window.review_datasets_tab)
                self.app.processEvents()
                compact_sizes = window.review_dataset_splitter.sizes()

                window.resize(1400, 700)
                self.app.processEvents()
                wide_sizes = window.review_dataset_splitter.sizes()

                self.assertGreater(wide_sizes[0], compact_sizes[0])
                self.assertGreater(wide_sizes[1], compact_sizes[1])
            finally:
                window.close()

    def test_manage_data_workflow_columns_render_side_by_side(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.resize(1200, 700)
                window.show()
                window.tabs.setCurrentWidget(window.info_sources_tab)
                self.app.processEvents()

                workflow_layout = window.info_sources_tab.layout().itemAt(3).layout()
                self.assertIsInstance(workflow_layout, QHBoxLayout)
                grouping_geometry = window.grouping_group_box.geometry()
                actions_geometry = window.dataset_actions_group_box.geometry()

                self.assertGreater(grouping_geometry.width(), 0)
                self.assertGreater(actions_geometry.width(), 0)
                self.assertLess(grouping_geometry.left(), actions_geometry.left())
                self.assertLessEqual(abs(grouping_geometry.width() - actions_geometry.width()), 48)
            finally:
                window.close()

    def test_activity_log_is_written_to_data_out_log_txt(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.append_activity_log("Loaded catalog.", activity="Load from database")

                    log_path = Path(tmp) / "data_out" / "log.txt"

                    self.assertTrue(log_path.exists())
                    contents = log_path.read_text(encoding="utf-8")
                    self.assertRegex(
                        contents,
                        r"^\[\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\] \[Load from database\] - Loaded catalog\.\n?$",
                    )
            finally:
                window.close()

    def test_refresh_all_views_after_worker_logs_refresh_stage(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                called = []
                window.refresh_all_views = lambda: called.append("refreshed")

                window._refresh_all_views_after_worker("Fast Local Classification")

                self.assertEqual(called, ["refreshed"])
                self.assertIn(
                    "[Fast Local Classification] - Applying results to the catalog and refreshing views.",
                    window.log_text.toPlainText(),
                )
            finally:
                window.close()

    def test_scan_result_leaves_fast_local_classification_for_review(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    captured = {}
                    window._start_worker_with_refresh = lambda fn, dataset_ids, success_message, **kwargs: captured.update(
                        {
                            "fn_name": getattr(fn, "__name__", ""),
                            "dataset_ids": list(dataset_ids),
                            "success_message": success_message,
                            "activity_name": kwargs.get("activity_name"),
                        }
                    )
                    dataset = DatasetRecord(
                        dataset_id="auto1",
                        source_path="D:/data/auto1.geojson",
                        source_format="geojson",
                        layer_name="Administrativo Distritos",
                        cache_path="auto1.parquet",
                        fingerprint="abc",
                    )

                    window.on_scan_result([dataset])

                    self.assertEqual(captured, {})
                    self.assertIn(
                        "1 new or changed dataset(s) are ready for Find info (fast) in Manage data.",
                        window.log_text.toPlainText(),
                    )
            finally:
                window.close()

    def test_start_scan_passes_collect_available_metadata_flag_to_worker(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    captured: dict[str, object] = {}

                    class _Signal:
                        def connect(self, _fn) -> None:
                            return None

                    class _Worker:
                        def __init__(self, fn, *args, **kwargs) -> None:
                            captured["fn"] = fn
                            captured["args"] = args
                            captured["kwargs"] = kwargs
                            self.signals = SimpleNamespace(
                                status=_Signal(),
                                progress=_Signal(),
                                result=_Signal(),
                                error=_Signal(),
                                finished=_Signal(),
                            )

                    started: list[object] = []
                    window.folder_edit.setText(tmp)
                    window.collect_available_metadata_checkbox.setChecked(True)

                    with patch("grasp.ui.main_window.FunctionWorker", _Worker), patch.object(
                        window.thread_pool,
                        "start",
                        side_effect=lambda worker: started.append(worker),
                    ):
                        window.start_scan()

                    self.assertIs(captured["fn"], window.ingest_service.scan_folder)
                    self.assertEqual(captured["args"][:2], (tmp, []))
                    self.assertEqual(captured["kwargs"], {"collect_available_metadata": True})
                    self.assertEqual(len(started), 1)
            finally:
                window.close()

    def test_scan_result_can_simplify_long_import_names(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.simplify_import_names_checkbox.setChecked(True)
                    dataset = DatasetRecord(
                        dataset_id="long1",
                        source_path="D:/data/pndt_vectors.gpkg",
                        source_format="gpkg",
                        layer_name="cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
                        cache_path="long1.parquet",
                        fingerprint="abc",
                    )

                    window.on_scan_result([dataset])

                    stored = window.repository.get_dataset("long1")
                    self.assertEqual(stored.display_name_user, "Posto fronteira")
                    self.assertIn("Source naming context: Cartografia tematica > Toponimia.", stored.description_user)
                    self.assertEqual(window.import_table.item(0, 0).text(), "Posto fronteira")
            finally:
                window.close()

    def test_scan_result_keeps_source_name_when_simplification_is_disabled(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    dataset = DatasetRecord(
                        dataset_id="long1",
                        source_path="D:/data/pndt_vectors.gpkg",
                        source_format="gpkg",
                        layer_name="cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
                        cache_path="long1.parquet",
                        fingerprint="abc",
                    )

                    window.on_scan_result([dataset])

                    stored = window.repository.get_dataset("long1")
                    self.assertEqual(stored.display_name_user, "")
                    self.assertEqual(
                        window.import_table.item(0, 0).text(),
                        "cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
                    )
            finally:
                window.close()

    def test_scan_result_does_not_override_existing_manual_name_when_simplifying(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="long1",
                                source_path="D:/data/pndt_vectors.gpkg",
                                source_format="gpkg",
                                layer_name="cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
                                display_name_user="Border posts",
                                description_user="Manual description",
                                cache_path="long1.parquet",
                                fingerprint="old",
                            )
                        ]
                    )
                    window.simplify_import_names_checkbox.setChecked(True)
                    dataset = DatasetRecord(
                        dataset_id="long1",
                        source_path="D:/data/pndt_vectors.gpkg",
                        source_format="gpkg",
                        layer_name="cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
                        cache_path="long1.parquet",
                        fingerprint="new",
                    )

                    window.on_scan_result([dataset])

                    stored = window.repository.get_dataset("long1")
                    self.assertEqual(stored.display_name_user, "Border posts")
                    self.assertEqual(stored.description_user, "Manual description")
            finally:
                window.close()

    def test_scan_result_processing_is_scheduled_outside_worker_result_slot(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                dataset = DatasetRecord(
                    dataset_id="auto1",
                    source_path="D:/data/auto1.geojson",
                    source_format="geojson",
                    cache_path="auto1.parquet",
                )
                with patch("grasp.ui.main_window.QTimer.singleShot") as single_shot:
                    window._schedule_scan_result(7, [dataset])

                self.assertEqual(single_shot.call_count, 1)
                delay_ms, scheduled_fn = single_shot.call_args.args
                self.assertEqual(delay_ms, 0)
                self.assertTrue(callable(scheduled_fn))
            finally:
                window.close()

    def test_complete_scan_result_finishes_background_activity_after_catalog_refresh(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    token = window._begin_background_activity("Rebuilding archive...", activity="Rebuild archive")
                    dataset = DatasetRecord(
                        dataset_id="auto1",
                        source_path="D:/data/auto1.geojson",
                        source_format="geojson",
                        cache_path="auto1.parquet",
                        fingerprint="abc",
                    )

                    window._complete_scan_result(token, [dataset])

                    self.assertEqual(window._active_background_progress_token, 0)
                    self.assertEqual(window.log_button.text(), "Logs")
                    self.assertIn("[Rebuild archive] - ending", window.log_text.toPlainText())
            finally:
                window.close()

    def test_fast_local_classification_stops_after_one_minute_budget(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Administrativo Distritos", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Patrimonio Parque Nacional", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", layer_name="Zona Costeira", cache_path="c.parquet"),
                        ]
                    )
                    messages: list[str] = []

                    with patch("grasp.ui.main_window.monotonic", side_effect=[0.0, 0.0, 61.0]):
                        processed = window._heuristic_classify_dataset_ids(
                            ["a", "b", "c"],
                            status_callback=messages.append,
                        )

                    self.assertEqual(processed, 1)
                    self.assertTrue(window.repository.get_dataset("a").display_name_ai)
                    self.assertEqual(window.repository.get_dataset("b").display_name_ai, "")
                    self.assertEqual(window.repository.get_dataset("c").display_name_ai, "")
                    self.assertTrue(
                        any("Fast local classification has a 01:00 time budget for this pass." in message for message in messages)
                    )
                    self.assertTrue(
                        any(
                            "Fast local classification time budget reached after 01:01. Leaving 2 dataset(s) unchanged for now."
                            in message
                            for message in messages
                        )
                    )
                    self.assertTrue(
                        any(
                            "Fast local classification finished this pass after processing 1/3 dataset(s)." in message
                            for message in messages
                        )
                    )
            finally:
                window.close()

    def test_refresh_after_worker_is_scheduled_outside_finished_job(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with patch("grasp.ui.main_window.QTimer.singleShot") as single_shot:
                    window._schedule_refresh_all_views_after_worker("Generate Styles")

                self.assertEqual(single_shot.call_count, 1)
                delay_ms, scheduled_fn = single_shot.call_args.args
                self.assertEqual(delay_ms, 0)
                self.assertTrue(callable(scheduled_fn))
            finally:
                window.close()

    def test_map_loading_is_paused_while_review_job_is_running(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )

                    window._review_job_running = True
                    window.tabs.setCurrentWidget(window.map_tab)
                    window.refresh_map()

                    self.assertFalse(window._map_initialized)
                    self.assertTrue(window._map_refresh_pending)
                    self.assertIn("paused while dataset processing is running", window.map_summary.text())

                    window._on_review_job_finished("AI classification completed.")

                    self.assertTrue(window._map_initialized)
                    self.assertFalse(window._map_refresh_pending)
            finally:
                window.close()

    def test_refresh_map_waits_for_webengine_page_before_publishing_state(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                                visibility=True,
                            )
                        ]
                    )
                    window._map_initialized = True
                    window._map_page_ready = False
                    calls: list[str] = []
                    window.map_bridge = SimpleNamespace(
                        set_scope=lambda scope: calls.append(f"scope:{scope}"),
                        publish_state=lambda: calls.append("publish"),
                    )

                    with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", True):
                        window.tabs.setCurrentWidget(window.map_tab)
                        window.refresh_map()

                    self.assertIn("Preparing the embedded map renderer", window.map_summary.text())
                    self.assertIn("scope:visible", calls)
                    self.assertNotIn("publish", calls)
                    self.assertTrue(window._map_refresh_pending)
            finally:
                window.close()

    def test_refresh_map_checked_scope_uses_checked_working_set_count(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", visibility=True, cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", visibility=True, cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", visibility=False, cache_path="c.parquet"),
                        ]
                    )
                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(0).setCheckState(0, Qt.Checked)
                    checked_scope_index = window.map_scope_combo.findData("checked")
                    self.assertGreaterEqual(checked_scope_index, 0)
                    window.map_scope_combo.setCurrentIndex(checked_scope_index)

                    window.refresh_map()

                    self.assertIn("Map layers in current scope (Checked working set): 1 of 3", window.map_summary.text())
                    self.assertIn("Checked working set: 1", window.map_summary.text())
            finally:
                window.close()

    def test_map_page_loaded_triggers_pending_refresh(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window._map_refresh_pending = True
                window.tabs.setCurrentWidget(window.map_tab)
                window._review_job_running = False
                calls: list[str] = []
                window.refresh_map = lambda: calls.append("refresh")

                window._on_map_view_loaded(True)

                self.assertTrue(window._map_page_ready)
                self.assertEqual(calls, ["refresh"])
                self.assertIn("[Map] - Embedded map page loaded.", window.log_text.toPlainText())
            finally:
                window.close()

    def test_run_ai_all_worker_logic_also_populates_sources(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="ds1",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                geometry_type="LineString",
                                feature_count=2,
                                fingerprint="abc",
                                cache_path="data_out/cache/datasets/ds1.parquet",
                            )
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        classify=lambda dataset: DatasetUnderstanding(
                            suggested_title="AI Roads",
                            suggested_description="AI description",
                            suggested_group="transport",
                            search_queries=["roads dataset"],
                            confidence=0.8,
                        )
                    )
                    window.search_service = SimpleNamespace(
                        find_sources=lambda understanding, dataset: [
                            SourceCandidate(
                                url="https://example.org/roads",
                                title="Roads source",
                                confidence=0.9,
                                is_selected=True,
                                candidate_id="src1",
                            )
                        ]
                    )

                    processed = window._classify_and_search_dataset_ids(["ds1"])

                    self.assertEqual(processed, 1)
                    self.assertEqual(window.repository.get_dataset("ds1").display_name_ai, "AI Roads")
                    self.assertEqual(len(window.repository.list_sources("ds1")), 1)
            finally:
                window.close()

    def test_rebuild_ai_services_uses_search_settings(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.current_settings.managed_data_language = "Portuguese"
                window.current_settings.classification_include_sample_values = True
                window.current_settings.classification_include_geometry_type = True
                window.current_settings.search_timeout_s = 2.5
                window.current_settings.search_max_consecutive_failures = 3
                window.current_settings.search_target_candidates = 9

                window._rebuild_ai_services()

                classifier = window.intelligence_service.classifier
                self.assertEqual(classifier.managed_data_language, "Portuguese")
                self.assertTrue(classifier.include_sample_values)
                self.assertTrue(classifier.include_geometry_type)
                provider = window.search_service.provider
                self.assertEqual(provider.timeout_s, 2.5)
                self.assertEqual(provider.max_consecutive_failures, 3)
                self.assertEqual(provider.target_candidates, 9)
            finally:
                window.close()

    def test_save_settings_persists_managed_data_language(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                portuguese_index = window.settings_data_language_combo.findData("Portuguese")
                self.assertGreaterEqual(portuguese_index, 0)

                window.settings_data_language_combo.setCurrentIndex(portuguese_index)
                window.save_settings()

                self.assertEqual(window.current_settings.managed_data_language, "Portuguese")
                self.assertEqual(window.intelligence_service.classifier.managed_data_language, "Portuguese")
                self.assertIn("Data language: portuguese", window.settings_model_label.text())
            finally:
                window.close()

    def test_ai_runtime_note_describes_sequential_timeout_budget(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.current_settings.openai_api_key = "test-key"
                window._rebuild_ai_services()
                note = window._ai_runtime_note(307)
                self.assertIn("run sequentially for 307 dataset(s)", note)
                timeout_value = int(round(window._openai_provider().timeout_s))
                self.assertIn(f"per-dataset timeout is {timeout_value}s", note)
                self.assertIn(window._format_elapsed_seconds(timeout_value * 307), note)
                self.assertIn("0.35s cooldown", note)
            finally:
                window.close()

    def test_ai_runtime_note_reports_missing_api_key_and_fallback(self) -> None:
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False), patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.current_settings.openai_api_key = ""
                window._rebuild_ai_services()
                note = window._ai_runtime_note(12)
                self.assertIn("OpenAI API key is missing", note)
                self.assertIn("heuristic fallback for 12 dataset(s)", note)
            finally:
                window.close()

    def test_manual_ai_understanding_status_mentions_waiting_for_ai_response(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                class _Response:
                    def raise_for_status(self) -> None:
                        return None

                    def json(self) -> dict:
                        return {
                            "choices": [
                                {
                                    "message": {
                                        "content": '{"theme":"coastal","keywords":["zoneamento","erosao"],'
                                        '"place_names":[],"suggested_title":"AI title",'
                                        '"suggested_description":"AI description",'
                                        '"suggested_group":"coastal","search_queries":["erosao buffer"],'
                                        '"confidence":0.8}'
                                    }
                                }
                            ]
                        }

                class _Session:
                    def post(self, *_args, **_kwargs):
                        return _Response()

                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                layer_name="Zoneamento Erosao Buffer 300",
                                cache_path="a.parquet",
                            )
                        ]
                    )
                    messages: list[str] = []
                    provider = OpenAIClassificationProvider(
                        api_key="test-key",
                        session=_Session(),
                    )
                    window.intelligence_service = IntelligenceService(
                        classifier=provider,
                    )

                    processed = window._classify_dataset_ids(["a"], status_callback=messages.append)

                    self.assertEqual(processed, 1)
                    self.assertTrue(
                        any(
                            "Finding info with AI 1/1: Zoneamento Erosao Buffer 300 (waiting for AI response)"
                            in message
                            for message in messages
                        )
                    )
                    self.assertFalse(
                        any("using heuristic fallback" in message for message in messages)
                    )
                    self.assertTrue(
                        any(
                            "Completed finding info with ai for 1/1 dataset(s)." in message
                            for message in messages
                        )
                    )
            finally:
                window.close()

    def test_manual_ai_understanding_logs_heuristic_fallback_when_api_key_is_missing(self) -> None:
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False), patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.current_settings.openai_api_key = ""
                window._rebuild_ai_services()
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                layer_name="Zoneamento Erosao Buffer 300",
                                cache_path="a.parquet",
                            )
                        ]
                    )
                    messages: list[str] = []

                    processed = window._classify_dataset_ids(["a"], status_callback=messages.append)

                    self.assertEqual(processed, 1)
                    self.assertTrue(
                        any(
                            "Finding info with AI 1/1: Zoneamento Erosao Buffer 300 (using heuristic fallback)"
                            in message
                            for message in messages
                        )
                    )
                    self.assertTrue(
                        any("OpenAI API key is missing" in message for message in messages)
                    )
            finally:
                window.close()

    def test_classification_batches_catalog_persistence_and_logs_it(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", layer_name="Layer A", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", layer_name="Layer B", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="c", source_format="geojson", layer_name="Layer C", cache_path="c.parquet"),
                        ]
                    )
                    messages: list[str] = []
                    batch_sizes: list[int] = []
                    auto_assign_flags: list[bool] = []
                    original_bulk_upsert = window.repository.upsert_understandings_bulk

                    def _capture_bulk_upsert(updates, *, auto_assign_group=False):
                        batch_sizes.append(len(updates))
                        auto_assign_flags.append(bool(auto_assign_group))
                        return original_bulk_upsert(updates, auto_assign_group=auto_assign_group)

                    window.intelligence_service = SimpleNamespace(
                        classify=lambda dataset: DatasetUnderstanding(
                            suggested_title=f"AI {dataset.layer_name}",
                            suggested_description="AI description",
                            suggested_group="coastal",
                            confidence=0.8,
                        )
                    )
                    window.repository.upsert_understandings_bulk = _capture_bulk_upsert

                    with patch("grasp.ui.main_window.UNDERSTANDING_PERSIST_BATCH_SIZE", 2):
                        processed = window._classify_dataset_ids(["a", "b", "c"], status_callback=messages.append)

                    self.assertEqual(processed, 3)
                    self.assertEqual(batch_sizes, [2, 1])
                    self.assertEqual(auto_assign_flags, [False, False])
                    self.assertTrue(
                        any("Persisting 2 understanding update(s) to the catalog." in message for message in messages)
                    )
                    self.assertTrue(
                        any("Persisting 1 understanding update(s) to the catalog." in message for message in messages)
                    )
            finally:
                window.close()

    def test_find_info_ai_keeps_existing_group_membership_unchanged(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                layer_name="Zoneamento Erosao Buffer 300",
                                group_id="ungrouped",
                                cache_path="a.parquet",
                            )
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        classify=lambda dataset: DatasetUnderstanding(
                            suggested_title="AI title",
                            suggested_description="AI description",
                            suggested_group="coastal",
                            confidence=0.8,
                        )
                    )

                    processed = window._classify_dataset_ids(["a"])

                    stored = window.repository.get_dataset("a")
                    self.assertEqual(processed, 1)
                    self.assertEqual(stored.display_name_ai, "AI title")
                    self.assertEqual(stored.suggested_group, "coastal")
                    self.assertEqual(stored.group_id, "ungrouped")
                    self.assertNotIn(("coastal", "Coastal"), window.repository.list_groups())
            finally:
                window.close()

    def test_style_generation_stores_styles_for_checked_datasets(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/parque.gpkg",
                                source_format="gpkg",
                                layer_name="Patrimonio Parque Nacional",
                                description_ai="Protected area boundaries",
                                geometry_type="MultiPolygon",
                                cache_path="a.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)

                    processed = window._style_dataset_ids(["a"])

                    self.assertEqual(processed, 1)
                    stored_style = window.repository.get_style("a")
                    self.assertIsNotNone(stored_style)
                    self.assertEqual(stored_style.theme, "protected-area")
            finally:
                window.close()

    def test_style_generation_uses_intelligence_when_available(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/misc.gpkg",
                                source_format="gpkg",
                                layer_name="Camada",
                                geometry_type="MultiLineString",
                                cache_path="a.parquet",
                            )
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        classify=lambda dataset: DatasetUnderstanding(
                            suggested_title=dataset.layer_name,
                            suggested_description="River network",
                            suggested_group="hydrology",
                            confidence=0.8,
                        )
                    )

                    processed = window._style_dataset_ids(["a"])

                    self.assertEqual(processed, 1)
                    stored_style = window.repository.get_style("a")
                    self.assertIsNotNone(stored_style)
                    self.assertEqual(stored_style.theme, "hydrology")
            finally:
                window.close()

    def test_style_generation_reports_missing_datasets_in_summary(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/parque.gpkg",
                                source_format="gpkg",
                                layer_name="Patrimonio Parque Nacional",
                                geometry_type="MultiPolygon",
                                cache_path="a.parquet",
                            )
                        ]
                    )
                    messages: list[str] = []

                    processed = window._style_dataset_ids(["a", "missing"], status_callback=messages.append)

                    self.assertEqual(processed, 1)
                    self.assertTrue(any("Skipping missing dataset 2/2: missing" == message for message in messages))
                    self.assertTrue(
                        any("Styled 1/2 dataset(s). Skipped 1 missing dataset(s)." == message for message in messages)
                    )
            finally:
                window.close()

    def test_start_style_for_scope_warns_when_source_styling_is_detected(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="styled",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                source_style_summary="Possible source styling detected: QGIS QML style file (roads.qml).",
                                source_style_items_json='[{"kind":"sidecar:qml","label":"QGIS QML style file (roads.qml)","path":"D:/data/roads.qml"}]',
                                cache_path="styled.parquet",
                            )
                        ]
                    )
                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    started = []
                    window._start_worker_with_refresh = lambda *args, **kwargs: started.append("started")

                    with patch("grasp.ui.main_window.QMessageBox.question", return_value=QMessageBox.No):
                        window.start_style_for_scope()

                    self.assertEqual(started, [])
            finally:
                window.close()

    def test_start_style_for_scope_can_continue_after_source_style_warning(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="styled",
                                source_path="D:/data/roads.geojson",
                                source_format="geojson",
                                source_style_summary="Possible source styling detected: QGIS QML style file (roads.qml).",
                                source_style_items_json='[{"kind":"sidecar:qml","label":"QGIS QML style file (roads.qml)","path":"D:/data/roads.qml"}]',
                                cache_path="styled.parquet",
                            )
                        ]
                    )
                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    started = []
                    window._start_worker_with_refresh = lambda *args, **kwargs: started.append("started")

                    with patch("grasp.ui.main_window.QMessageBox.question", return_value=QMessageBox.Yes):
                        window.start_style_for_scope()

                    self.assertEqual(started, ["started"])
            finally:
                window.close()

    def test_start_style_for_scope_uses_checked_working_set(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="checked",
                                source_path="D:/data/checked.geojson",
                                source_format="geojson",
                                visibility=False,
                                cache_path="checked.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="visible",
                                source_path="D:/data/visible.geojson",
                                source_format="geojson",
                                visibility=True,
                                cache_path="visible.parquet",
                            ),
                        ]
                    )
                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)

                    captured: list[tuple[str, list[str]]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append((fn.__name__, dataset_ids))
                    )

                    with patch.object(window, "_confirm_style_generation_for_dataset_ids", return_value=True):
                        window.start_style_for_scope()

                    self.assertEqual(captured, [("_style_dataset_ids", ["checked"])])
            finally:
                window.close()

    def test_fill_checked_user_fields_from_ai_only_fills_empty_fields(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                display_name_ai="AI A",
                                description_ai="AI Desc A",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                display_name_user="Manual B",
                                description_ai="AI Desc B",
                                display_name_ai="AI B",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(0).setCheckState(0, Qt.Checked)
                    first_group.child(1).setCheckState(0, Qt.Checked)

                    window.fill_checked_user_fields_from_ai()

                    stored_a = window.repository.get_dataset("a")
                    stored_b = window.repository.get_dataset("b")
                    self.assertEqual(stored_a.display_name_user, "AI A")
                    self.assertEqual(stored_a.description_user, "AI Desc A")
                    self.assertEqual(stored_b.display_name_user, "Manual B")
                    self.assertEqual(stored_b.description_user, "AI Desc B")
            finally:
                window.close()

    def test_transfer_ai_to_checked_overwrites_existing_user_fields(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                display_name_user="Manual A",
                                description_user="Manual Desc A",
                                display_name_ai="AI A",
                                description_ai="AI Desc A",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                display_name_user="Manual B",
                                description_user="Manual Desc B",
                                display_name_ai="AI B",
                                description_ai="AI Desc B",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(0).setCheckState(0, Qt.Checked)

                    window.transfer_ai_to_checked()

                    stored_a = window.repository.get_dataset("a")
                    stored_b = window.repository.get_dataset("b")
                    self.assertEqual(stored_a.display_name_user, "AI A")
                    self.assertEqual(stored_a.description_user, "AI Desc A")
                    self.assertEqual(stored_b.display_name_user, "Manual B")
                    self.assertEqual(stored_b.description_user, "Manual Desc B")
            finally:
                window.close()

    def test_make_visible_in_maps_enables_visibility_for_checked_datasets(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                visibility=False,
                                include_in_export=False,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                visibility=False,
                                include_in_export=False,
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(0).setCheckState(0, Qt.Checked)

                    window.make_checked_visible_in_maps()

                    self.assertTrue(window.repository.get_dataset("a").visibility)
                    self.assertFalse(window.repository.get_dataset("b").visibility)
                    self.assertFalse(window.repository.get_dataset("a").include_in_export)
                    self.assertFalse(window.repository.get_dataset("b").include_in_export)
                    self.assertEqual(window._map_scope(), "checked")
            finally:
                window.close()

    def test_hide_from_maps_disables_visibility_for_checked_datasets(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                visibility=True,
                                include_in_export=False,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                visibility=True,
                                include_in_export=False,
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(0).setCheckState(0, Qt.Checked)

                    window.hide_checked_from_maps()

                    self.assertFalse(window.repository.get_dataset("a").visibility)
                    self.assertTrue(window.repository.get_dataset("b").visibility)
            finally:
                window.close()

    def test_visibility_checkbox_persists_immediately_for_selected_dataset(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                visibility=False,
                                include_in_export=False,
                                cache_path="a.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    window.tree.setCurrentItem(first_group.child(0))

                    window.visibility_checkbox.setChecked(True)
                    window.include_export_checkbox.setChecked(True)

                    stored = window.repository.get_dataset("a")
                    self.assertIsNotNone(stored)
                    self.assertTrue(stored.visibility)
                    self.assertTrue(stored.include_in_export)
            finally:
                window.close()

    def test_include_in_report_marks_checked_datasets_for_export(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                include_in_export=False,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                include_in_export=False,
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(1).setCheckState(0, Qt.Checked)

                    window.include_checked_in_report()

                    self.assertFalse(window.repository.get_dataset("a").include_in_export)
                    self.assertTrue(window.repository.get_dataset("b").include_in_export)
            finally:
                window.close()

    def test_exclude_from_report_unmarks_checked_datasets_for_export(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                include_in_export=True,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                include_in_export=True,
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    first_group = window.tree.topLevelItem(0)
                    first_group.child(1).setCheckState(0, Qt.Checked)

                    window.exclude_checked_from_report()

                    self.assertTrue(window.repository.get_dataset("a").include_in_export)
                    self.assertFalse(window.repository.get_dataset("b").include_in_export)
            finally:
                window.close()

    def test_transfer_ai_to_all_overwrites_all_datasets_with_ai_text(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                display_name_user="Manual A",
                                description_user="Manual Desc A",
                                display_name_ai="AI A",
                                description_ai="AI Desc A",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                display_name_user="Manual B",
                                description_user="Manual Desc B",
                                display_name_ai="AI B",
                                description_ai="AI Desc B",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    window.transfer_ai_to_all()

                    stored_a = window.repository.get_dataset("a")
                    stored_b = window.repository.get_dataset("b")
                    self.assertEqual(stored_a.display_name_user, "AI A")
                    self.assertEqual(stored_a.description_user, "AI Desc A")
                    self.assertEqual(stored_b.display_name_user, "AI B")
                    self.assertEqual(stored_b.description_user, "AI Desc B")
            finally:
                window.close()

    def test_regroup_worker_assigns_requested_number_of_groups(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/admin_districts.geojson", source_format="geojson", layer_name="Administrativo Distritos", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/admin_capital.geojson", source_format="geojson", layer_name="Administrativo Capital", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/patrimonio_parque.geojson", source_format="geojson", layer_name="Patrimonio Parque Nacional", cache_path="c.parquet"),
                            DatasetRecord(dataset_id="d", source_path="D:/data/patrimonio_reserva.geojson", source_format="geojson", layer_name="Patrimonio Reserva Nacional", cache_path="d.parquet"),
                        ]
                    )

                    regrouped = window._regroup_dataset_ids(["a", "b", "c", "d"], 2)

                    self.assertEqual(regrouped, 4)
                    groups = {window.repository.get_dataset(dataset_id).group_id for dataset_id in ["a", "b", "c", "d"]}
                    self.assertEqual(len(groups), 2)
            finally:
                window.close()

    def test_start_regroup_uses_preview_job(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", cache_path="b.parquet"),
                        ]
                    )
                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    captured: dict[str, object] = {}
                    window._start_regroup_preview_job = lambda dataset_ids, target_group_count, *, scope_label: captured.update(
                        {
                            "dataset_ids": list(dataset_ids),
                            "target_group_count": target_group_count,
                            "scope_label": scope_label,
                        }
                    )

                    with patch.object(window, "_prompt_group_count", return_value=2):
                        window.start_regroup_for_scope("checked")

                    self.assertEqual(
                        captured,
                        {"dataset_ids": ["a"], "target_group_count": 2, "scope_label": "checked datasets"},
                    )
            finally:
                window.close()

    def test_confirm_regroup_assignments_shows_group_preview(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Administrative Districts", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Capital Cities", cache_path="b.parquet"),
                        ]
                    )

                    dialog = window._create_regroup_confirmation_dialog({"a": "Administrative", "b": "Administrative"})
                    self.assertIsNotNone(dialog)
                    summary_label = dialog.findChild(QLabel, "regroupSummaryLabel")
                    preview_box = dialog.findChild(QPlainTextEdit, "regroupPreviewBox")
                    question_label = dialog.findChild(QLabel, "regroupQuestionLabel")

                    self.assertIsNotNone(summary_label)
                    self.assertIsNotNone(preview_box)
                    self.assertIsNotNone(question_label)
                    self.assertEqual(dialog.windowTitle(), "Review regroup proposal")
                    self.assertIn("AI Regroup proposed 1 group(s) for 2 dataset(s).", summary_label.text())
                    self.assertIn("Administrative (2)", preview_box.toPlainText())
                    self.assertIn("Administrative Districts", preview_box.toPlainText())
                    self.assertIn("Capital Cities", preview_box.toPlainText())
                    self.assertIn("drag datasets between groups in the datasets overview tab", question_label.text().lower())

                    with patch("grasp.ui.main_window.QDialog.exec", return_value=QDialog.Rejected):
                        accepted = window._confirm_regroup_assignments({"a": "Administrative", "b": "Administrative"})

                    self.assertFalse(accepted)
            finally:
                window.close()

    def test_regroup_preview_shows_all_groups_and_datasets_without_truncation(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    dataset_specs = [
                        ("a1", "Admin One"),
                        ("a2", "Admin Two"),
                        ("a3", "Admin Three"),
                        ("a4", "Admin Four"),
                        ("a5", "Admin Five"),
                        ("b1", "Boundary Layer"),
                        ("c1", "City Layer"),
                        ("d1", "River Layer"),
                        ("e1", "Road Layer"),
                        ("f1", "Land Use Layer"),
                        ("g1", "Terrain Layer"),
                        ("h1", "Building Layer"),
                        ("i1", "Parcel Layer"),
                    ]
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id=dataset_id,
                                source_path=f"D:/data/{dataset_id}.geojson",
                                source_format="geojson",
                                layer_name=layer_name,
                                cache_path=f"{dataset_id}.parquet",
                            )
                            for dataset_id, layer_name in dataset_specs
                        ]
                    )

                    assignments = {
                        "a1": "Administrative",
                        "a2": "Administrative",
                        "a3": "Administrative",
                        "a4": "Administrative",
                        "a5": "Administrative",
                        "b1": "Boundaries",
                        "c1": "Cities",
                        "d1": "Rivers",
                        "e1": "Roads",
                        "f1": "Land Use",
                        "g1": "Terrain",
                        "h1": "Buildings",
                        "i1": "Parcels",
                    }

                    dialog = window._create_regroup_confirmation_dialog(assignments)
                    self.assertIsNotNone(dialog)
                    preview_box = dialog.findChild(QPlainTextEdit, "regroupPreviewBox")

                    self.assertIsNotNone(preview_box)
                    preview_text = preview_box.toPlainText()
                    self.assertIn("Administrative (5)", preview_text)
                    self.assertIn("Admin Five", preview_text)
                    self.assertIn("Admin Four", preview_text)
                    self.assertIn("Admin One", preview_text)
                    self.assertIn("Admin Three", preview_text)
                    self.assertIn("Admin Two", preview_text)
                    self.assertIn("Boundaries (1)", preview_text)
                    self.assertIn("Boundary Layer", preview_text)
                    self.assertIn("Buildings (1)", preview_text)
                    self.assertIn("Building Layer", preview_text)
                    self.assertIn("Cities (1)", preview_text)
                    self.assertIn("City Layer", preview_text)
                    self.assertIn("Land Use (1)", preview_text)
                    self.assertIn("Land Use Layer", preview_text)
                    self.assertIn("Parcels (1)", preview_text)
                    self.assertIn("Parcel Layer", preview_text)
                    self.assertIn("Rivers (1)", preview_text)
                    self.assertIn("River Layer", preview_text)
                    self.assertIn("Roads (1)", preview_text)
                    self.assertIn("Road Layer", preview_text)
                    self.assertIn("Terrain (1)", preview_text)
                    self.assertIn("Terrain Layer", preview_text)
                    self.assertNotIn("more dataset(s)", preview_text)
                    self.assertNotIn("more proposed group(s)", preview_text)
            finally:
                window.close()

    def test_review_tab_mentions_drag_and_drop_grouping(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertIn("Drag datasets between groups below", window.datasets_help_note.text())
            finally:
                window.close()

    def test_regroup_does_not_reclassify_existing_ungrouped_datasets_before_grouping(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                display_name_ai="AI A",
                                description_ai="AI Desc A",
                                suggested_group="ungrouped",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                display_name_ai="AI B",
                                description_ai="AI Desc B",
                                suggested_group="ungrouped",
                                cache_path="b.parquet",
                            ),
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        classify=lambda dataset: (_ for _ in ()).throw(AssertionError("Unexpected reclassification")),
                        group_datasets=lambda datasets, target_group_count: {
                            dataset.dataset_id: ("Group 1" if dataset.dataset_id == "a" else "Group 2")
                            for dataset in datasets
                        },
                    )

                    regrouped = window._regroup_dataset_ids(["a", "b"], 2)

                    self.assertEqual(regrouped, 2)
                    self.assertEqual(window.repository.get_dataset("a").group_id, "group-1")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "group-2")
            finally:
                window.close()

    def test_regroup_assigns_unmatched_datasets_to_others_and_logs_phases(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Protected Area", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Coastal Buffer", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", layer_name="Fishing Zones", cache_path="c.parquet"),
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        group_datasets=lambda datasets, target_group_count: {"a": "Protected Area"}
                    )
                    messages: list[str] = []
                    progress_values: list[int] = []

                    regrouped = window._regroup_dataset_ids(
                        ["a", "b", "c"],
                        3,
                        status_callback=messages.append,
                        progress_callback=progress_values.append,
                    )

                    self.assertEqual(regrouped, 3)
                    self.assertEqual(window.repository.get_dataset("a").group_id, "protected-area")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "others")
                    self.assertEqual(window.repository.get_dataset("c").group_id, "others")
                    self.assertTrue(any("Preparing fresh regroup input for 3 dataset(s)." in message for message in messages))
                    self.assertTrue(any("Starting group synthesis for 3 dataset(s) with target 3 group(s)." in message for message in messages))
                    self.assertTrue(any("Grouping response covered 1/3 prepared dataset(s)." in message for message in messages))
                    self.assertTrue(any("Assigning 2 dataset(s) to Others." in message for message in messages))
                    self.assertTrue(any("Applying 3 group assignment(s) to the catalog." in message for message in messages))
                    self.assertTrue(any("Regroup complete: 3 dataset(s) assigned across 2 populated group(s)." in message for message in messages))
                    self.assertEqual(progress_values[-1], 100)
            finally:
                window.close()

    def test_regroup_ignores_cached_ai_group_hints_and_existing_groups(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Legacy Group")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="D:/data/a.geojson",
                                source_format="geojson",
                                layer_name="Distritos",
                                display_name_ai="Administrative Regions",
                                description_ai="Old AI description",
                                suggested_group="administrative",
                                group_id="legacy-group",
                                cache_path="a.parquet",
                            ),
                        ]
                    )
                    captured: dict[str, DatasetRecord] = {}

                    def _group_datasets(datasets, target_group_count, timeout_s=None):
                        captured["dataset"] = datasets[0]
                        return {"a": "Novo Grupo"}

                    window.intelligence_service = SimpleNamespace(group_datasets=_group_datasets)
                    messages: list[str] = []

                    proposal = window._prepare_regroup_assignments(["a"], 1, status_callback=messages.append)

                    self.assertEqual(proposal["assignments"]["a"], "Novo Grupo")
                    self.assertIn("dataset", captured)
                    self.assertEqual(captured["dataset"].display_name_ai, "")
                    self.assertEqual(captured["dataset"].description_ai, "")
                    self.assertEqual(captured["dataset"].suggested_group, "")
                    self.assertEqual(captured["dataset"].display_name_user, "")
                    self.assertEqual(captured["dataset"].layer_name, "Distritos")
                    self.assertTrue(
                        any(
                            "Current group assignments and cached AI grouping hints will be ignored for this run."
                            in message
                            for message in messages
                        )
                    )
                    self.assertTrue(
                        any(
                            "User-entered names and descriptions were kept; cached AI grouping hints were ignored."
                            in message
                            for message in messages
                        )
                    )
            finally:
                window.close()

    def test_regroup_timeout_budget_sends_remaining_datasets_to_others(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Protected Area", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Coastal Buffer", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", layer_name="Fishing Zones", cache_path="c.parquet"),
                        ]
                    )
                    window.intelligence_service = SimpleNamespace(
                        group_datasets=lambda datasets, target_group_count: {"a": "Protected Area"}
                    )
                    messages: list[str] = []

                    with patch("grasp.ui.main_window.monotonic", side_effect=[0.0, 0.0, 241.0, 241.0]):
                        regrouped = window._regroup_dataset_ids(["a", "b", "c"], 3, status_callback=messages.append)

                    self.assertEqual(regrouped, 3)
                    self.assertEqual(window.repository.get_dataset("a").group_id, "others")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "others")
                    self.assertEqual(window.repository.get_dataset("c").group_id, "others")
                    self.assertTrue(
                        any(
                            "Regroup time budget reached during hint preparation after 04:01. Assigning the remaining 2 dataset(s) to Others."
                            in message
                            for message in messages
                        )
                    )
            finally:
                window.close()

    def test_regroup_passes_remaining_budget_into_grouping_step(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Protected Area", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Coastal Buffer", cache_path="b.parquet"),
                        ]
                    )
                    captured: dict[str, float] = {}

                    def _group_datasets(datasets, target_group_count, timeout_s=None, group_count_bounds=None):
                        captured["timeout_s"] = float(timeout_s or 0.0)
                        return {dataset.dataset_id: "Grouped" for dataset in datasets}

                    window.intelligence_service = SimpleNamespace(group_datasets=_group_datasets)
                    messages: list[str] = []

                    with patch("grasp.ui.main_window.monotonic", side_effect=[0.0] + [10.0] * 8):
                        regrouped = window._regroup_dataset_ids(["a", "b"], 2, status_callback=messages.append)

                    self.assertEqual(regrouped, 2)
                    self.assertAlmostEqual(captured["timeout_s"], 230.0, places=2)
                    self.assertTrue(
                        any("Waiting for grouping response (max 03:50 remaining)." in message for message in messages)
                    )
            finally:
                window.close()

    def test_regroup_retries_with_more_groups_when_result_is_too_broad(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="risk-1", source_path="D:/data/risk1.geojson", source_format="geojson", layer_name="Risco de inundacao alto", cache_path="risk1.parquet"),
                            DatasetRecord(dataset_id="risk-2", source_path="D:/data/risk2.geojson", source_format="geojson", layer_name="Risco de seca alto", cache_path="risk2.parquet"),
                            DatasetRecord(dataset_id="risk-3", source_path="D:/data/risk3.geojson", source_format="geojson", layer_name="Risco de erosao alto", cache_path="risk3.parquet"),
                            DatasetRecord(dataset_id="protected-1", source_path="D:/data/protected1.geojson", source_format="geojson", layer_name="Parque Nacional", cache_path="protected1.parquet"),
                            DatasetRecord(dataset_id="protected-2", source_path="D:/data/protected2.geojson", source_format="geojson", layer_name="Reserva Especial", cache_path="protected2.parquet"),
                            DatasetRecord(dataset_id="protected-3", source_path="D:/data/protected3.geojson", source_format="geojson", layer_name="Zona tampo", cache_path="protected3.parquet"),
                            DatasetRecord(dataset_id="admin-1", source_path="D:/data/admin1.geojson", source_format="geojson", layer_name="Distritos", cache_path="admin1.parquet"),
                            DatasetRecord(dataset_id="admin-2", source_path="D:/data/admin2.geojson", source_format="geojson", layer_name="Provincia designacao", cache_path="admin2.parquet"),
                            DatasetRecord(dataset_id="admin-3", source_path="D:/data/admin3.geojson", source_format="geojson", layer_name="Distrito localizacao", cache_path="admin3.parquet"),
                            DatasetRecord(dataset_id="transport-1", source_path="D:/data/transport1.geojson", source_format="geojson", layer_name="Rede viaria", cache_path="transport1.parquet"),
                            DatasetRecord(dataset_id="transport-2", source_path="D:/data/transport2.geojson", source_format="geojson", layer_name="Rede ferroviaria", cache_path="transport2.parquet"),
                            DatasetRecord(dataset_id="transport-3", source_path="D:/data/transport3.geojson", source_format="geojson", layer_name="Aeroportos", cache_path="transport3.parquet"),
                        ]
                    )
                    calls: list[int] = []

                    def _group_datasets(datasets, target_group_count, timeout_s=None):
                        calls.append(int(target_group_count))
                        if len(calls) == 1:
                            return {dataset.dataset_id: "Mega Group" for dataset in datasets}
                        return {
                            dataset.dataset_id: (
                                "Risk" if dataset.dataset_id.startswith("risk-")
                                else "Protected Area" if dataset.dataset_id.startswith("protected-")
                                else "Administrative" if dataset.dataset_id.startswith("admin-")
                                else "Transport"
                            )
                            for dataset in datasets
                        }

                    window.intelligence_service = SimpleNamespace(
                        classifier=HeuristicClassificationProvider(),
                        group_datasets=_group_datasets,
                    )
                    messages: list[str] = []

                    assignments = window._group_datasets_for_regroup(
                        [window.repository.get_dataset(dataset_id) for dataset_id in [
                            "risk-1", "risk-2", "risk-3",
                            "protected-1", "protected-2", "protected-3",
                            "admin-1", "admin-2", "admin-3",
                            "transport-1", "transport-2", "transport-3",
                        ]],
                        4,
                        status_callback=messages.append,
                        timeout_s=60.0,
                    )

                    self.assertEqual(calls, [4, 6])
                    self.assertEqual(assignments["risk-1"], "Risk")
                    self.assertEqual(assignments["protected-1"], "Protected Area")
                    self.assertTrue(
                        any("Retrying with suggested target 6 group(s)." in message for message in messages)
                    )
                    self.assertTrue(
                        any("Using regroup retry result with target 6 group(s)." in message for message in messages)
                    )
            finally:
                window.close()

    def test_regroup_with_hard_retry_ceiling_does_not_escalate_beyond_user_choice(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Risco de inundacao alto", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Reserva Especial", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", layer_name="Distritos", cache_path="c.parquet"),
                            DatasetRecord(dataset_id="d", source_path="D:/data/d.geojson", source_format="geojson", layer_name="Rede viaria", cache_path="d.parquet"),
                        ]
                    )
                    calls: list[int] = []

                    def _group_datasets(datasets, target_group_count, timeout_s=None, group_count_bounds=None):
                        calls.append(int(target_group_count))
                        return {dataset.dataset_id: f"Group {index}" for index, dataset in enumerate(datasets, start=1)}

                    window.intelligence_service = SimpleNamespace(
                        classifier=HeuristicClassificationProvider(),
                        group_datasets=_group_datasets,
                    )
                    messages: list[str] = []

                    assignments = window._group_datasets_for_regroup(
                        [window.repository.get_dataset(dataset_id) for dataset_id in ["a", "b", "c", "d"]],
                        4,
                        regroup_policy={
                            "variance_prompt_enabled": False,
                            "max_attempt_count": 1,
                            "hard_max_target_group_count": 4,
                            "group_count_bounds": (4, 4),
                        },
                        status_callback=messages.append,
                        timeout_s=60.0,
                    )

                    self.assertEqual(calls, [4])
                    self.assertEqual(len(set(assignments.values())), 4)
                    self.assertFalse(
                        any("Retrying with suggested target" in message for message in messages)
                    )
            finally:
                window.close()

    def test_complete_regroup_preview_applies_assignments_after_confirmation(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", group_id="ungrouped", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", group_id="ungrouped", cache_path="b.parquet"),
                        ]
                    )
                    token = window._begin_background_activity("Regrouping checked datasets...", activity="AI Regroup")
                    window._review_job_running = True

                    with patch.object(window, "_confirm_regroup_assignments", return_value=True):
                        window._complete_regroup_preview(token, {"assignments": {"a": "Administrative", "b": "Protected Area"}})

                    self.assertEqual(window.repository.get_dataset("a").group_id, "administrative")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "protected-area")
                    self.assertFalse(window._review_job_running)
            finally:
                window.close()

    def test_regroup_group_count_bounds_allow_ten_percent_variance(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertEqual(window._regroup_group_count_bounds(40, 237), (36, 44))
            finally:
                window.close()

    def test_complete_regroup_preview_retries_when_group_count_exceeds_tolerance(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    token = window._begin_background_activity("Regrouping checked datasets...", activity="AI Regroup")
                    window._review_job_running = True
                    captured: dict[str, object] = {}

                    with patch.object(
                        window,
                        "_resolve_regroup_group_count_variance",
                        return_value={
                            "action": "retry",
                            "target_group_count": 68,
                            "regroup_policy": {
                                "variance_prompt_enabled": False,
                                "max_attempt_count": 1,
                            },
                            "message": "Retrying with the higher AI-suggested target of 68 groups.",
                        },
                    ), patch.object(
                        window,
                        "_start_regroup_preview_job",
                        side_effect=lambda dataset_ids, target_group_count, *, scope_label, regroup_policy=None: captured.update(
                            {
                                "dataset_ids": dataset_ids,
                                "target_group_count": target_group_count,
                                "scope_label": scope_label,
                                "regroup_policy": regroup_policy,
                            }
                        ),
                    ), patch.object(window, "_confirm_regroup_assignments", side_effect=AssertionError("Should not confirm before retry")):
                        window._complete_regroup_preview(
                            token,
                            {
                                "assignments": {f"ds-{index}": f"Group {index}" for index in range(1, 69)},
                                "dataset_ids": [f"ds-{index}" for index in range(1, 69)],
                                "target_group_count": 40,
                            },
                            "checked datasets",
                        )

                    self.assertEqual(
                        captured,
                        {
                            "dataset_ids": [f"ds-{index}" for index in range(1, 69)],
                            "target_group_count": 68,
                            "scope_label": "checked datasets",
                            "regroup_policy": {
                                "variance_prompt_enabled": False,
                                "max_attempt_count": 1,
                            },
                        },
                    )
                    self.assertFalse(window._review_job_running)
            finally:
                window.close()

    def test_complete_regroup_preview_skips_second_variance_prompt_after_locked_retry_choice(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", group_id="ungrouped", cache_path="a.parquet"),
                        ]
                    )
                    token = window._begin_background_activity("Regrouping checked datasets...", activity="AI Regroup")
                    window._review_job_running = True

                    with patch.object(
                        window,
                        "_resolve_regroup_group_count_variance",
                        side_effect=AssertionError("Locked rerun should not prompt again"),
                    ), patch.object(window, "_confirm_regroup_assignments", return_value=True):
                        window._complete_regroup_preview(
                            token,
                            {
                                "assignments": {"a": "Administrative"},
                                "dataset_ids": ["a"],
                                "target_group_count": 40,
                                "regroup_policy": {
                                    "variance_prompt_enabled": False,
                                    "max_attempt_count": 1,
                                },
                            },
                            "checked datasets",
                        )

                    self.assertEqual(window.repository.get_dataset("a").group_id, "administrative")
                    self.assertFalse(window._review_job_running)
            finally:
                window.close()

    def test_complete_regroup_preview_replaces_existing_groups_and_refreshes_tree(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Legacy Group")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", group_id="legacy-group", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", group_id="legacy-group", cache_path="b.parquet"),
                        ]
                    )
                    window.refresh_all_views()
                    token = window._begin_background_activity("Regrouping checked datasets...", activity="AI Regroup")
                    window._review_job_running = True

                    with patch.object(window, "_confirm_regroup_assignments", return_value=True):
                        window._complete_regroup_preview(token, {"assignments": {"a": "Administrative", "b": "Protected Area"}})

                    self.assertNotIn(("legacy-group", "Legacy Group"), window.repository.list_groups())
                    top_level_groups = [window.tree.topLevelItem(index).text(0) for index in range(window.tree.topLevelItemCount())]
                    self.assertIn("Administrative", top_level_groups)
                    self.assertIn("Protected Area", top_level_groups)
                    self.assertNotIn("Legacy Group", top_level_groups)
            finally:
                window.close()

    def test_complete_regroup_preview_keeps_groups_unchanged_when_cancelled(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", group_id="transport", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", group_id="ungrouped", cache_path="b.parquet"),
                        ]
                    )
                    token = window._begin_background_activity("Regrouping checked datasets...", activity="AI Regroup")
                    window._review_job_running = True

                    with patch.object(window, "_confirm_regroup_assignments", return_value=False):
                        window._complete_regroup_preview(token, {"assignments": {"a": "Administrative", "b": "Protected Area"}})

                    self.assertEqual(window.repository.get_dataset("a").group_id, "transport")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "ungrouped")
                    self.assertFalse(window._review_job_running)
            finally:
                window.close()

    def test_reset_groups_for_scope_moves_checked_datasets_to_ungrouped_and_prunes_empty_groups(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Legacy Group")
                    window.repository.create_group("Keep Group")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", group_id="legacy-group", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", group_id="legacy-group", cache_path="b.parquet"),
                            DatasetRecord(dataset_id="c", source_path="D:/data/c.geojson", source_format="geojson", group_id="keep-group", cache_path="c.parquet"),
                        ]
                    )
                    window.refresh_all_views()
                    legacy_group = next(
                        window.tree.topLevelItem(index)
                        for index in range(window.tree.topLevelItemCount())
                        if window.tree.topLevelItem(index).text(0) == "Legacy Group"
                    )
                    legacy_group.child(0).setCheckState(0, Qt.Checked)
                    legacy_group.child(1).setCheckState(0, Qt.Checked)

                    with patch("grasp.ui.main_window.QMessageBox.question", return_value=QMessageBox.Yes):
                        window.reset_groups_for_scope()

                    self.assertEqual(window.repository.get_dataset("a").group_id, "ungrouped")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "ungrouped")
                    self.assertEqual(window.repository.get_dataset("c").group_id, "keep-group")
                    self.assertNotIn(("legacy-group", "Legacy Group"), window.repository.list_groups())
                    top_level_groups = [window.tree.topLevelItem(index).text(0) for index in range(window.tree.topLevelItemCount())]
                    self.assertIn("Ungrouped", top_level_groups)
                    self.assertIn("Keep Group", top_level_groups)
                    self.assertNotIn("Legacy Group", top_level_groups)
            finally:
                window.close()

    def test_regroup_uses_local_grouping_when_openai_is_unavailable(self) -> None:
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False), patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                window.current_settings.openai_api_key = ""
                window._rebuild_ai_services()
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Protected Area", cache_path="a.parquet"),
                            DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Coastal Buffer", cache_path="b.parquet"),
                        ]
                    )
                    messages: list[str] = []

                    regrouped = window._regroup_dataset_ids(["a", "b"], 2, status_callback=messages.append)

                    self.assertEqual(regrouped, 2)
                    self.assertTrue(
                        any(
                            "OpenAI API key is missing. Configure it in Settings to use Find info (AI). Using local grouping fallback."
                            in message
                            for message in messages
                        )
                    )
            finally:
                window.close()

    def test_selection_buttons_update_checked_state_without_touching_visibility(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Transport")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="b.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="c",
                                source_path="c",
                                source_format="geojson",
                                group_id="ungrouped",
                                cache_path="c.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    window.set_all_checked(False)
                    self.assertEqual(window._checked_dataset_ids(), [])
                    self.assertTrue(window.repository.get_dataset("a").visibility)
                    self.assertTrue(window.repository.get_dataset("b").visibility)
                    self.assertTrue(window.repository.get_dataset("c").visibility)

                    group_item = None
                    for index in range(window.tree.topLevelItemCount()):
                        candidate = window.tree.topLevelItem(index)
                        if candidate.text(0) == "Transport":
                            group_item = candidate
                            break
                    self.assertIsNotNone(group_item)
                    window.selection_group_combo.setCurrentIndex(window.selection_group_combo.findData("transport"))
                    window.set_selected_group_checked(True)

                    self.assertEqual(window._checked_dataset_ids(), ["a", "b"])
                    self.assertTrue(window.repository.get_dataset("a").visibility)
                    self.assertTrue(window.repository.get_dataset("b").visibility)
                    self.assertTrue(window.repository.get_dataset("c").visibility)
            finally:
                window.close()

    def test_manage_data_summary_reports_checked_group_count(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Transport")
                    window.repository.create_group("Hydro")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                group_id="hydro",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    for index in range(window.tree.topLevelItemCount()):
                        group_item = window.tree.topLevelItem(index)
                        if group_item.childCount() > 0:
                            group_item.child(0).setCheckState(0, Qt.Checked)

                    self.assertIn(
                        "Working set: 2 checked dataset(s), divided between 2 groups.",
                        window.selection_scope_status_label.text(),
                    )
                    self.assertIn("Dropdown group:", window.selection_scope_status_label.text())
            finally:
                window.close()

    def test_ai_action_uses_checked_datasets_in_checked_scope(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                visibility=True,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                visibility=False,
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    window.tree.setCurrentItem(None)
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    captured: list[tuple[str, list[str]]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append((fn.__name__, dataset_ids))
                    )

                    window.start_ai_for_scope("checked")

                    self.assertEqual(captured, [("_classify_dataset_ids", ["a"])])
            finally:
                window.close()

    def test_fast_info_action_uses_checked_scope_and_heuristic_worker(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    captured: list[tuple[str, list[str], str]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append(
                            (fn.__name__, dataset_ids, kwargs.get("activity_name", ""))
                        )
                    )

                    window.start_fast_info_for_scope("checked")

                    self.assertEqual(captured, [("_heuristic_classify_dataset_ids", ["a"], "Fast Local Classification")])
            finally:
                window.close()

    def test_ai_action_uses_all_scope_when_requested(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                visibility=True,
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                visibility=True,
                                cache_path="b.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="c",
                                source_path="c",
                                source_format="geojson",
                                visibility=False,
                                cache_path="c.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    captured: list[list[str]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append(dataset_ids)
                    )

                    window.start_ai_for_scope("all")

                    self.assertEqual(captured, [["a", "b", "c"]])
            finally:
                window.close()

    def test_find_sources_action_uses_search_worker(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                cache_path="a.parquet",
                            )
                        ]
                    )

                    window.refresh_all_views()
                    group_item = window.tree.topLevelItem(0)
                    group_item.child(0).setCheckState(0, Qt.Checked)
                    captured: list[tuple[str, list[str]]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append((fn.__name__, dataset_ids))
                    )

                    window.start_sources_for_scope("checked")

                    self.assertEqual(captured, [("_search_dataset_ids", ["a"])])
            finally:
                window.close()

    def test_ai_action_uses_checked_ungrouped_group_selection(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                group_id="ungrouped",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                group_id="ungrouped",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    ungrouped_item = window.tree.topLevelItem(0)
                    self.assertEqual(ungrouped_item.text(0), "Ungrouped")
                    ungrouped_item.setCheckState(0, Qt.Checked)
                    captured: list[list[str]] = []
                    window._start_worker_with_refresh = (
                        lambda fn, dataset_ids, success_message, **kwargs: captured.append(dataset_ids)
                    )

                    window.start_ai_for_scope("checked")

                    self.assertEqual(captured, [["a", "b"]])
            finally:
                window.close()

    def test_group_checkbox_reflects_checked_children(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.repository.create_group("Transport")
                    window.repository.replace_datasets(
                        [
                            DatasetRecord(
                                dataset_id="a",
                                source_path="a",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="b",
                                source_format="geojson",
                                group_id="transport",
                                cache_path="b.parquet",
                            ),
                        ]
                    )

                    window.refresh_all_views()
                    group_item = None
                    for index in range(window.tree.topLevelItemCount()):
                        candidate = window.tree.topLevelItem(index)
                        if candidate.text(0) == "Transport":
                            group_item = candidate
                            break
                    self.assertIsNotNone(group_item)
                    first_child = group_item.child(0)
                    second_child = group_item.child(1)
                    first_child.setCheckState(0, Qt.Checked)

                    self.assertEqual(group_item.checkState(0), Qt.PartiallyChecked)

                    second_child.setCheckState(0, Qt.Checked)

                    self.assertEqual(group_item.checkState(0), Qt.Checked)
                    self.assertEqual(window._checked_dataset_ids(), ["a", "b"])
            finally:
                window.close()


if __name__ == "__main__":
    unittest.main()

