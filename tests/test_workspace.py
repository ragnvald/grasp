from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grasp.workspace import catalog_exists, ensure_workspace, iter_supported_files


class WorkspaceTests(unittest.TestCase):
    def test_frozen_runtime_stores_workspace_beside_executable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app_dir = Path(tmp) / "portable-app"
            source_root = Path(tmp) / "archive"
            app_dir.mkdir()
            source_root.mkdir()

            with patch("grasp.workspace.sys.frozen", True, create=True), patch(
                "grasp.workspace.sys.executable",
                str(app_dir / "grasp-desktop.exe"),
            ):
                workspace = ensure_workspace(source_root)

                self.assertTrue(workspace.workspace_path.is_relative_to(app_dir / "data_out"))
                self.assertEqual(workspace.workspace_path.parent, app_dir / "data_out")
                self.assertEqual(workspace.root_path, source_root.resolve())
                self.assertTrue(workspace.cache_dir.exists())
                self.assertTrue(workspace.exports_dir.exists())
                self.assertTrue(workspace.logs_dir.exists())
                self.assertTrue(workspace.temp_dir.exists())

    def test_frozen_runtime_uses_distinct_workspace_per_source_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app_dir = Path(tmp) / "portable-app"
            first_root = Path(tmp) / "archive-a"
            second_root = Path(tmp) / "archive-b"
            app_dir.mkdir()
            first_root.mkdir()
            second_root.mkdir()

            with patch("grasp.workspace.sys.frozen", True, create=True), patch(
                "grasp.workspace.sys.executable",
                str(app_dir / "grasp-desktop.exe"),
            ):
                first = ensure_workspace(first_root)
                second = ensure_workspace(second_root)

                self.assertNotEqual(first.workspace_path, second.workspace_path)
                self.assertEqual(first.workspace_path.parent, second.workspace_path.parent)

    def test_frozen_runtime_catalog_exists_checks_portable_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app_dir = Path(tmp) / "portable-app"
            source_root = Path(tmp) / "archive"
            app_dir.mkdir()
            source_root.mkdir()

            with patch("grasp.workspace.sys.frozen", True, create=True), patch(
                "grasp.workspace.sys.executable",
                str(app_dir / "grasp-desktop.exe"),
            ):
                workspace = ensure_workspace(source_root)
                self.assertFalse(catalog_exists(source_root))
                workspace.db_path.write_text("", encoding="utf-8")
                self.assertTrue(catalog_exists(source_root))

    def test_frozen_runtime_does_not_exclude_source_side_data_out(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app_dir = Path(tmp) / "portable-app"
            source_root = Path(tmp) / "archive"
            app_dir.mkdir()
            source_root.mkdir()
            (source_root / "data_out").mkdir()
            (source_root / "data_out" / "kept.geojson").write_text("{}", encoding="utf-8")
            (source_root / "roads.geojson").write_text("{}", encoding="utf-8")

            with patch("grasp.workspace.sys.frozen", True, create=True), patch(
                "grasp.workspace.sys.executable",
                str(app_dir / "grasp-desktop.exe"),
            ):
                files = iter_supported_files(source_root)

            self.assertEqual([path.relative_to(source_root).as_posix() for path in files], ["data_out/kept.geojson", "roads.geojson"])


if __name__ == "__main__":
    unittest.main()
