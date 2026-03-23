from __future__ import annotations

from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path

from grasp.source_style import detect_source_style_evidence, summarize_source_style_evidence


class SourceStyleTests(unittest.TestCase):
    def test_detects_sidecar_style_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "roads.geojson"
            source_path.write_text("{}", encoding="utf-8")
            (root / "roads.qml").write_text("<qml/>", encoding="utf-8")

            evidence = detect_source_style_evidence(source_path)

            self.assertEqual(len(evidence), 1)
            self.assertIn("QGIS QML style file", evidence[0]["label"])
            self.assertIn("roads.qml", summarize_source_style_evidence(evidence))

    def test_detects_geopackage_style_tables_for_matching_layer(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            gpkg_path = Path(tmp) / "bundle.gpkg"
            with closing(sqlite3.connect(gpkg_path)) as conn:
                conn.execute("CREATE TABLE layer_styles (f_table_name TEXT)")
                conn.execute("INSERT INTO layer_styles (f_table_name) VALUES ('roads')")
                conn.commit()

            evidence = detect_source_style_evidence(gpkg_path, "roads")

            self.assertTrue(any("GeoPackage layer_styles table" in item["label"] for item in evidence))


if __name__ == "__main__":
    unittest.main()
