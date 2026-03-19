from __future__ import annotations

from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

from grasp.catalog.repository import CatalogRepository
from grasp.export.service import ExportService
from grasp.models import DatasetRecord, SourceCandidate
from grasp.workspace import ensure_workspace


class ExportServiceTests(unittest.TestCase):
    def test_export_geopackage_and_geoparquet(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            repo = CatalogRepository(workspace.db_path)

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

            service = ExportService(workspace, repo)
            gpkg_path = service.export_gpkg(workspace.exports_dir / "knowledge.gpkg")
            parquet_path = service.export_geoparquet(workspace.exports_dir / "knowledge.parquet")

            self.assertTrue(gpkg_path.exists())
            self.assertTrue(parquet_path.exists())

            exported = gpd.read_parquet(parquet_path)
            self.assertEqual(sorted(exported.columns.tolist()), sorted([
                "dataset_id",
                "dataset_name",
                "group_name",
                "description",
                "attributes_json",
                "source_url",
                "source_evidence_json",
                "geometry",
            ]))
            self.assertEqual(len(exported), 2)

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

            qgs_path = gpkg_path.with_suffix(".qgs")
            self.assertTrue(qgs_path.exists())
            self.assertIn("layer-tree-layer", qgs_path.read_text(encoding="utf-8"))

    def test_export_builds_missing_cache_from_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            repo = CatalogRepository(workspace.db_path)

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
                        column_profile_json='{"columns":[{"name":"name","dtype":"object","samples":["a"]}]}',
                        fingerprint="abc",
                        cache_path=str(cache_path),
                    )
                ]
            )

            service = ExportService(workspace, repo)
            parquet_path = service.export_geoparquet(workspace.exports_dir / "knowledge.parquet")

            self.assertTrue(parquet_path.exists())
            self.assertTrue(cache_path.exists())
            exported = gpd.read_parquet(parquet_path)
            self.assertEqual(len(exported), 2)


if __name__ == "__main__":
    unittest.main()

