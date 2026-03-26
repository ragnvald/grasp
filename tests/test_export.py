from __future__ import annotations

from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path
import zipfile
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point
from xml.etree import ElementTree as ET

from grasp.catalog.repository import CatalogRepository
import grasp.export.service as export_service_module
from grasp.export.service import ExportService
from grasp.models import DatasetRecord, SourceCandidate
from grasp.workspace import ensure_workspace


class ExportServiceTests(unittest.TestCase):
    def test_export_geopackage_writes_metadata_and_project_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            repo = CatalogRepository(workspace.db_path)
            template_dir = root / "data" / "qgis"
            template_dir.mkdir(parents=True, exist_ok=True)
            template_path = template_dir / "template.qgz"
            template_path.write_bytes(export_service_module.QGIS_TEMPLATE_QGZ_PATH.read_bytes())

            gdf = gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[Point(10.4, 63.4), Point(10.5, 63.41)],
                crs="EPSG:4326",
            )
            cache_path = workspace.dataset_cache_path("ds1")
            gdf.to_parquet(cache_path, index=False)

            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="ds1",
                        source_path=str(root / "points.geojson"),
                        source_format="geojson",
                        geometry_type="Point",
                        feature_count=2,
                        column_profile_json='{"columns":[{"name":"name","dtype":"object","samples":["a"]}]}',
                        fingerprint="abc",
                        cache_path=str(cache_path),
                    )
                ]
            )
            repo.save_dataset_user_fields("ds1", display_name_user="Points", description_user="Example", visibility=True, include_in_export=True)
            repo.replace_sources(
                "ds1",
                [
                    SourceCandidate(
                        url="https://example.org/points",
                        title="Points source",
                        domain="example.org",
                        confidence=0.9,
                        is_selected=True,
                        candidate_id="src1",
                    )
                ],
            )

            with patch.object(export_service_module, "QGIS_TEMPLATE_QGZ_PATH", template_path):
                service = ExportService(workspace, repo)
                gpkg_path = service.export_gpkg(workspace.exports_dir / "knowledge.gpkg")
            project_dir = template_dir

            self.assertTrue(gpkg_path.exists())

            with closing(sqlite3.connect(gpkg_path)) as conn:
                tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                self.assertIn("dataset_catalog", tables)
                self.assertIn("source_candidates", tables)
                self.assertIn("layer_styles", tables)
                self.assertIn("grasp_qgis_project", tables)
                dataset_catalog = conn.execute(
                    "SELECT style_theme, style_summary, style_json FROM dataset_catalog WHERE dataset_id = 'ds1'"
                ).fetchone()
                self.assertIsNotNone(dataset_catalog)
                self.assertTrue(dataset_catalog[0])
                self.assertTrue(dataset_catalog[1])
                self.assertTrue(dataset_catalog[2])
                project_row = conn.execute("SELECT project_xml FROM grasp_qgis_project").fetchone()
                self.assertIn("points", project_row[0].lower())

            exported = gpd.read_file(gpkg_path, layer="Points")
            self.assertEqual(len(exported), 2)

            qgs_path = project_dir / "knowledge_gpkg.qgs"
            qgz_path = project_dir / "knowledge_gpkg.qgz"
            self.assertTrue(qgs_path.exists())
            self.assertTrue(qgz_path.exists())
            qgs_text = qgs_path.read_text(encoding="utf-8")
            self.assertIn("layer-tree-layer", qgs_text)
            self.assertIn("OSM Standard", qgs_text)
            self.assertIn("../../", qgs_text)
            qgs_root = ET.fromstring(qgs_text)
            maplayers_by_name = {
                maplayer.findtext("layername"): maplayer
                for maplayer in qgs_root.findall(".//projectlayers/maplayer")
            }
            self.assertEqual(
                maplayers_by_name["OSM Standard"].findtext("datasource"),
                "type=xyz&zmin=0&zmax=19&url=https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            )
            self.assertEqual(
                maplayers_by_name["Bing Satellite"].findtext("datasource"),
                "type=xyz&zmin=1&zmax=18&url=https://ecn.t3.tiles.virtualearth.net/tiles/a{q}.jpeg?g%3D0%26dir%3Ddir_n'",
            )
            self.assertIsNotNone(maplayers_by_name["Points"].find("renderer-v2"))
            with zipfile.ZipFile(qgz_path) as archive:
                self.assertIn(qgs_path.name, archive.namelist())
                qgz_project = archive.read(qgs_path.name).decode("utf-8")
                self.assertIn("layer-tree-layer", qgz_project)
                self.assertIn("OSM Standard", qgz_project)
                self.assertIn("../../", qgz_project)

    def test_export_geopackage_builds_missing_cache_from_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            repo = CatalogRepository(workspace.db_path)
            template_dir = root / "data" / "qgis"
            template_dir.mkdir(parents=True, exist_ok=True)
            template_path = template_dir / "template.qgz"
            template_path.write_bytes(export_service_module.QGIS_TEMPLATE_QGZ_PATH.read_bytes())

            gdf = gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[Point(10.4, 63.4), Point(10.5, 63.41)],
                crs="EPSG:4326",
            )
            source_path = root / "points.geojson"
            gdf.to_file(source_path, driver="GeoJSON")
            cache_path = workspace.dataset_cache_path("ds1")

            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="ds1",
                        source_path=str(source_path),
                        source_format="geojson",
                        geometry_type="Point",
                        feature_count=2,
                        include_in_export=True,
                        display_name_user="Points",
                        column_profile_json='{"columns":[{"name":"name","dtype":"object","samples":["a"]}]}',
                        fingerprint="abc",
                        cache_path=str(cache_path),
                    )
                ]
            )

            with patch.object(export_service_module, "QGIS_TEMPLATE_QGZ_PATH", template_path):
                service = ExportService(workspace, repo)
                gpkg_path = service.export_gpkg(workspace.exports_dir / "knowledge.gpkg")

            self.assertTrue(gpkg_path.exists())
            self.assertTrue(cache_path.exists())
            exported = gpd.read_file(gpkg_path, layer="Points")
            self.assertEqual(len(exported), 2)


if __name__ == "__main__":
    unittest.main()
