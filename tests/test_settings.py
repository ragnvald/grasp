from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from grasp.data_languages import (
    MANAGED_DATA_LANGUAGE_NOT_SET_LABEL,
    MANAGED_DATA_LANGUAGE_OPTIONS,
    display_managed_data_language,
)
from grasp.qt_compat import QSettings
from grasp.settings import AppSettings, SettingsStore


class SettingsStoreTests(unittest.TestCase):
    def test_last_folder_is_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ini_path = Path(tmp) / "settings.ini"
            backend = QSettings(str(ini_path), QSettings.IniFormat)
            store = SettingsStore(backend)

            settings = AppSettings(
                last_folder="D:/data/example",
                openai_model="gpt-4o-mini",
                managed_data_language="Portuguese",
                classification_include_source_name=False,
                classification_include_layer_name=True,
                classification_include_column_names=True,
                classification_include_sample_values=True,
                classification_include_geometry_type=False,
                classification_include_feature_count=False,
                classification_include_bbox=True,
                search_timeout_s=3.5,
                search_max_consecutive_failures=2,
                search_target_candidates=7,
            )
            store.save(settings)

            reloaded = SettingsStore(QSettings(str(ini_path), QSettings.IniFormat)).load()
            self.assertEqual(reloaded.last_folder, "D:/data/example")
            self.assertEqual(reloaded.openai_model, "gpt-4o-mini")
            self.assertEqual(reloaded.managed_data_language, "Portuguese")
            self.assertFalse(reloaded.classification_include_source_name)
            self.assertTrue(reloaded.classification_include_sample_values)
            self.assertTrue(reloaded.classification_include_bbox)
            self.assertEqual(reloaded.search_timeout_s, 3.5)
            self.assertEqual(reloaded.search_max_consecutive_failures, 2)
            self.assertEqual(reloaded.search_target_candidates, 7)

    def test_legacy_settings_are_used_when_primary_store_is_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            primary_ini = Path(tmp) / "primary.ini"
            legacy_ini = Path(tmp) / "legacy.ini"
            primary = QSettings(str(primary_ini), QSettings.IniFormat)
            legacy = QSettings(str(legacy_ini), QSettings.IniFormat)
            legacy.setValue("openai/model", "gpt-4.1-mini")
            legacy.setValue("ui/last_folder", "D:/legacy")
            legacy.sync()

            store = SettingsStore(primary)
            store._legacy_settings = legacy

            loaded = store.load()

            self.assertEqual(loaded.openai_model, "gpt-4.1-mini")
            self.assertEqual(loaded.last_folder, "D:/legacy")
            self.assertEqual(loaded.managed_data_language, "")

    def test_managed_data_language_defaults_and_display_helpers(self) -> None:
        self.assertEqual(display_managed_data_language(""), MANAGED_DATA_LANGUAGE_NOT_SET_LABEL)
        self.assertEqual(display_managed_data_language("portuguese"), "Portuguese")
        self.assertEqual(list(MANAGED_DATA_LANGUAGE_OPTIONS), sorted(MANAGED_DATA_LANGUAGE_OPTIONS, key=str.casefold))


if __name__ == "__main__":
    unittest.main()

