from __future__ import annotations

from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from grasp.branding import APP_AUTHOR, APP_TAGLINE, APP_WINDOW_TITLE
from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate
from grasp.qt_compat import QApplication, Qt
from grasp.ui.main_window import MainWindow


class MainWindowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = QApplication.instance() or QApplication([])

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
                    self.assertIn("Open the Map / Export tab", window.map_summary.text())

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
                    self.assertFalse(window.load_existing_button.isEnabled())
                    self.assertFalse(window.reset_data_button.isEnabled())

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

                    self.assertTrue(window.load_existing_button.isEnabled())
                    self.assertTrue(window.reset_data_button.isEnabled())
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
                    self.assertIn("Checked datasets are selected for batch actions in Review", window.review_visibility_note.text())
            finally:
                window.close()

    def test_dataset_controls_live_inside_datasets_group_box(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                self.assertEqual([window.tabs.tabText(index) for index in range(window.tabs.count())], ["Import", "Review", "Map / Export", "Settings", "About"])
                self.assertEqual(window.datasets_group_box.title(), "Datasets")
                self.assertEqual(window.review_actions_group_box.title(), "AI & Sources")
                self.assertEqual(window.selection_group_box.title(), "Selection")
                self.assertEqual(window.grouping_group_box.title(), "Grouping")
                self.assertEqual(window.dataset_actions_group_box.title(), "Selection actions")
                self.assertEqual(window.ai_settings_group_box.title(), "AI Settings")
                self.assertEqual(window.search_settings_group_box.title(), "Search Settings")
                self.assertEqual(window.ai_context_group_box.title(), "AI Classification Context")
                self.assertEqual(window.show_all_button.text(), "Select All")
                self.assertEqual(window.hide_all_button.text(), "Clear All")
                self.assertEqual(window.show_group_button.text(), "Select Group")
                self.assertEqual(window.hide_group_button.text(), "Clear Group")
                self.assertEqual(window.run_ai_sources_button.text(), "Find info (AI)")
                self.assertEqual(window.find_sources_button.text(), "Find sources")
                self.assertEqual(window.regroup_button.text(), "AI Regroup...")
                self.assertEqual(window.generate_styles_button.text(), "Generate Styles")
                self.assertEqual(window.transfer_ai_selected_button.text(), "Transfer AI to Name + Description")
                self.assertEqual(window.save_dataset_button.text(), "Save Changes")
                self.assertEqual(window.fill_ai_fields_button.text(), "Fill Empty Fields from AI")
                self.assertEqual(window.make_visible_button.text(), "Make visible in maps")
                self.assertEqual(window.include_in_report_button.text(), "Include in report")
                self.assertEqual(window.transfer_ai_selected_button.maximumWidth(), 220)
                self.assertEqual(window.save_dataset_button.maximumWidth(), 150)
                self.assertIn("find info (ai) updates ai title", window.review_actions_note.text().lower())
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.new_group_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.apply_group_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.fill_ai_fields_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.make_visible_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.include_in_report_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.regroup_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.show_all_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.hide_all_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.show_group_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.hide_group_button))
                self.assertTrue(window.datasets_group_box.isAncestorOf(window.tree))
                self.assertTrue(window.review_actions_group_box.isAncestorOf(window.run_ai_sources_button))
                self.assertTrue(window.review_actions_group_box.isAncestorOf(window.find_sources_button))
                self.assertTrue(window.review_actions_group_box.isAncestorOf(window.review_scope_combo))
                self.assertTrue(window.review_actions_group_box.isAncestorOf(window.review_actions_note))
                self.assertTrue(window.grouping_group_box.isAncestorOf(window.grouping_scope_combo))
                group_box_titles = [group_box.title() for group_box in window.findChildren(type(window.datasets_group_box))]
                self.assertNotIn("Source Candidates", group_box_titles)
                self.assertFalse(hasattr(window, "sources_table"))
                self.assertEqual(window.dataset_description_edit.minimumHeight(), 72)
                self.assertEqual(window.dataset_description_edit.maximumHeight(), 96)
                self.assertEqual(window.ai_description_box.minimumHeight(), 72)
                self.assertEqual(window.ai_description_box.maximumHeight(), 96)
                self.assertTrue(hasattr(window, "settings_search_timeout_edit"))
                self.assertTrue(hasattr(window, "settings_search_failures_edit"))
                self.assertTrue(hasattr(window, "settings_search_candidates_edit"))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.settings_model_combo))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.settings_timeout_edit))
                self.assertTrue(window.ai_settings_group_box.isAncestorOf(window.ai_context_group_box))
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

                    self.assertEqual(window.dataset_name_edit.text(), "")
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
                self.assertEqual(window.about_tagline_label.text(), APP_TAGLINE)
                self.assertIn(APP_AUTHOR, window.about_author_label.text())
                self.assertIn("Shapefile, GeoPackage and GeoParquet", window.about_purpose_label.text())
                self.assertIn("exports a packaged GeoPackage", window.about_capabilities_label.text())
                self.assertIn("structured knowledge", window.about_note_label.text())
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

    def test_activity_log_is_written_to_data_out_log_txt(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    window._set_workspace(tmp)
                    window.append_activity_log("Loaded catalog.", activity="Load Existing")

                    log_path = Path(tmp) / "data_out" / "log.txt"

                    self.assertTrue(log_path.exists())
                    contents = log_path.read_text(encoding="utf-8")
                    self.assertRegex(
                        contents,
                        r"^\[\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\] \[Load Existing\] - Loaded catalog\.\n?$",
                    )
            finally:
                window.close()

    def test_refresh_all_views_after_worker_logs_refresh_stage(self) -> None:
        with patch("grasp.ui.main_window.WEBENGINE_AVAILABLE", False):
            window = MainWindow()
            try:
                called = []
                window.refresh_all_views = lambda: called.append("refreshed")

                window._refresh_all_views_after_worker("Initial Heuristic Classification")

                self.assertEqual(called, ["refreshed"])
                self.assertIn(
                    "[Initial Heuristic Classification] - Applying results to the catalog and refreshing views.",
                    window.log_text.toPlainText(),
                )
            finally:
                window.close()

    def test_scan_result_queues_heuristic_auto_classification(self) -> None:
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

                    self.assertEqual(captured["fn_name"], "_heuristic_classify_dataset_ids")
                    self.assertEqual(captured["dataset_ids"], ["auto1"])
                    self.assertEqual(captured["activity_name"], "Initial Heuristic Classification")
                    self.assertIn(
                        "Queued initial heuristic classification for new or changed datasets with a 1-minute time budget.",
                        window.log_text.toPlainText(),
                    )
            finally:
                window.close()

    def test_initial_heuristic_classification_stops_after_one_minute_budget(self) -> None:
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
                        any("Heuristic classification has a 01:00 time budget for this automatic pass." in message for message in messages)
                    )
                    self.assertTrue(
                        any(
                            "Heuristic classification time budget reached after 01:01. Leaving 2 dataset(s) unchanged for now."
                            in message
                            for message in messages
                        )
                    )
                    self.assertTrue(
                        any(
                            "Heuristic classification finished the automatic pass after processing 1/3 dataset(s)." in message
                            for message in messages
                        )
                    )
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
                window.current_settings.classification_include_sample_values = True
                window.current_settings.classification_include_geometry_type = True
                window.current_settings.search_timeout_s = 2.5
                window.current_settings.search_max_consecutive_failures = 3
                window.current_settings.search_target_candidates = 9

                window._rebuild_ai_services()

                classifier = window.intelligence_service.classifier
                self.assertTrue(classifier.include_sample_values)
                self.assertTrue(classifier.include_geometry_type)
                provider = window.search_service.provider
                self.assertEqual(provider.timeout_s, 2.5)
                self.assertEqual(provider.max_consecutive_failures, 3)
                self.assertEqual(provider.target_candidates, 9)
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
                                cache_path="a.parquet",
                            ),
                            DatasetRecord(
                                dataset_id="b",
                                source_path="D:/data/b.geojson",
                                source_format="geojson",
                                visibility=False,
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
                    self.assertTrue(any("Preparing grouping hints for 3 dataset(s)." in message for message in messages))
                    self.assertTrue(any("Starting group synthesis for 3 dataset(s) with target 3 group(s)." in message for message in messages))
                    self.assertTrue(any("Grouping response covered 1/3 prepared dataset(s)." in message for message in messages))
                    self.assertTrue(any("Assigning 2 dataset(s) to Others." in message for message in messages))
                    self.assertTrue(any("Applying 3 group assignment(s) to the catalog." in message for message in messages))
                    self.assertTrue(any("Regroup complete: 3 dataset(s) assigned across 2 populated group(s)." in message for message in messages))
                    self.assertEqual(progress_values[-1], 100)
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

                    with patch("grasp.ui.main_window.monotonic", side_effect=[0.0, 0.0, 121.0, 121.0]):
                        regrouped = window._regroup_dataset_ids(["a", "b", "c"], 3, status_callback=messages.append)

                    self.assertEqual(regrouped, 3)
                    self.assertEqual(window.repository.get_dataset("a").group_id, "others")
                    self.assertEqual(window.repository.get_dataset("b").group_id, "others")
                    self.assertEqual(window.repository.get_dataset("c").group_id, "others")
                    self.assertTrue(
                        any(
                            "Regroup time budget reached during hint preparation after 02:01. Assigning the remaining 2 dataset(s) to Others."
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

                    def _group_datasets(datasets, target_group_count, timeout_s=None):
                        captured["timeout_s"] = float(timeout_s or 0.0)
                        return {dataset.dataset_id: "Grouped" for dataset in datasets}

                    window.intelligence_service = SimpleNamespace(group_datasets=_group_datasets)
                    messages: list[str] = []

                    with patch("grasp.ui.main_window.monotonic", side_effect=[0.0, 10.0, 10.0, 10.0]):
                        regrouped = window._regroup_dataset_ids(["a", "b"], 2, status_callback=messages.append)

                    self.assertEqual(regrouped, 2)
                    self.assertAlmostEqual(captured["timeout_s"], 110.0, places=2)
                    self.assertTrue(
                        any("Waiting for grouping response (max 01:50 remaining)." in message for message in messages)
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
                    window.tree.setCurrentItem(group_item)
                    window.set_selected_group_checked(True)

                    self.assertEqual(window._checked_dataset_ids(), ["a", "b"])
                    self.assertTrue(window.repository.get_dataset("a").visibility)
                    self.assertTrue(window.repository.get_dataset("b").visibility)
                    self.assertTrue(window.repository.get_dataset("c").visibility)
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

                    window.review_scope_combo.setCurrentIndex(window.review_scope_combo.findData("checked"))
                    window.start_ai_for_scope()

                    self.assertEqual(captured, [("_classify_dataset_ids", ["a"])])
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

                    window.review_scope_combo.setCurrentIndex(window.review_scope_combo.findData("all"))
                    window.start_ai_for_scope()

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

                    window.review_scope_combo.setCurrentIndex(window.review_scope_combo.findData("checked"))
                    window.start_sources_for_scope()

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

                    window.review_scope_combo.setCurrentIndex(window.review_scope_combo.findData("checked"))
                    window.start_ai_for_scope()

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

