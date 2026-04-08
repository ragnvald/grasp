from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point, Polygon

from grasp.ingest.service import IngestService
from grasp.workspace import ensure_workspace, iter_supported_files


class IngestServiceTests(unittest.TestCase):
    def test_scan_mixed_folder_and_gpkg_layers(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a", "b"], "kommune": ["Trondheim", "Trondheim"]},
                geometry=[Point(10.4, 63.4), Point(10.5, 63.41)],
                crs="EPSG:4326",
            )
            polygons = gpd.GeoDataFrame(
                {"zone": ["north"]},
                geometry=[Polygon([(10.3, 63.3), (10.6, 63.3), (10.6, 63.5), (10.3, 63.5)])],
                crs="EPSG:4326",
            )

            points.to_file(root / "points.shp")
            points.to_file(root / "bundle.gpkg", layer="places", driver="GPKG")
            polygons.to_file(root / "bundle.gpkg", layer="zones", driver="GPKG")
            points.to_file(root / "points.geojson", driver="GeoJSON")
            points.to_parquet(root / "points.parquet", index=False)

            service = IngestService()
            datasets = service.scan_folder(root)

            self.assertEqual(len(datasets), 5)
            self.assertEqual(sum(1 for dataset in datasets if dataset.source_format == "gpkg"), 2)
            for dataset in datasets:
                self.assertTrue(dataset.cache_path)
                service.ensure_dataset_cache(dataset)
                self.assertTrue(Path(dataset.cache_path).exists())
                self.assertTrue(dataset.geometry_type)

    def test_scan_assigns_default_crs_and_normalizes_invalid_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bowtie = Polygon([(0, 0), (2, 2), (0, 2), (2, 0), (0, 0)])
            gdf = gpd.GeoDataFrame({"name": ["invalid"]}, geometry=[bowtie], crs=None)
            gdf.to_parquet(root / "invalid.parquet", index=False)

            service = IngestService()
            datasets = service.scan_folder(root)

            self.assertEqual(len(datasets), 1)
            dataset = datasets[0]
            self.assertEqual(dataset.crs.upper(), "EPSG:4326")
            service.ensure_dataset_cache(dataset)
            cached = gpd.read_parquet(dataset.cache_path)
            self.assertEqual(len(cached), 1)
            self.assertTrue(bool(cached.geometry.iloc[0].is_valid))

    def test_scan_skips_dataset_with_unusable_wgs84_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            projected_like = gpd.GeoDataFrame(
                {"name": ["projected"]},
                geometry=[Point(500000.0, 8500000.0)],
                crs=None,
            )
            projected_like.to_parquet(root / "south_luangwa.parquet", index=False)

            service = IngestService()
            messages: list[str] = []
            datasets = service.scan_folder(root, status_callback=messages.append)

            self.assertEqual(datasets, [])
            self.assertTrue(
                any(
                    "failed geometry quality check: bounds are outside usable WGS84 range" in message
                    for message in messages
                )
            )

    def test_ensure_dataset_cache_rebuilds_empty_cached_preview(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")

            service = IngestService(workspace)
            datasets = service.scan_folder(root)
            self.assertEqual(len(datasets), 1)
            dataset = datasets[0]
            cache_path = workspace.resolve_cache_path(dataset.dataset_id, dataset.cache_path)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            points.head(0).to_parquet(cache_path, index=False)

            messages: list[str] = []
            rebuilt_cache = service.ensure_dataset_cache(dataset, status_callback=messages.append)

            cached = gpd.read_parquet(rebuilt_cache)
            self.assertEqual(len(cached), 1)
            self.assertTrue(
                any("Rebuilding cached preview for points.geojson" in message for message in messages)
            )

    def test_fingerprint_matches_equivalent_datasets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[Point(10.4, 63.4), Point(10.5, 63.41)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")
            points.to_parquet(root / "points.parquet", index=False)

            service = IngestService()
            datasets = sorted(service.scan_folder(root), key=lambda item: item.source_format)

            self.assertEqual(len(datasets), 2)
            self.assertEqual(datasets[0].fingerprint, datasets[1].fingerprint)

    def test_scan_ignores_non_geojson_json_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")
            (root / "settings.json").write_text(json.dumps({"app": "grasp", "version": 1}), encoding="utf-8")

            service = IngestService()
            datasets = service.scan_folder(root)

            self.assertEqual(len(datasets), 1)
            self.assertEqual(datasets[0].source_format, "geojson")

    def test_large_json_sniff_does_not_need_full_parse(self) -> None:
        service = IngestService()
        payload = '{"type":"FeatureCollection","features":[' + (' ' * 70000)
        self.assertTrue(service._looks_like_geojson_text(payload))

    def test_scan_reuses_unchanged_existing_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a", "b"]},
                geometry=[Point(10.4, 63.4), Point(10.5, 63.41)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")

            service = IngestService()
            first = service.scan_folder(root)

            with patch.object(service, "_summarize_dataset", side_effect=AssertionError("should not resummarize unchanged data")):
                second = service.scan_folder(root, first)

            self.assertEqual(len(second), 1)
            self.assertEqual(second[0].dataset_id, first[0].dataset_id)
            self.assertEqual(second[0].fingerprint, first[0].fingerprint)

    def test_scan_detects_sidecar_source_styling(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")
            (root / "points.qml").write_text("<qml/>", encoding="utf-8")

            service = IngestService()
            datasets = service.scan_folder(root)

            self.assertEqual(len(datasets), 1)
            self.assertTrue(datasets[0].has_source_style)
            self.assertIn("QGIS QML style file", datasets[0].source_style_summary)

    def test_scan_can_collect_associated_xml_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.shp")
            (root / "points.shp.xml").write_text(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><metadata><idinfo><citation>Road points</citation></idinfo></metadata>",
                encoding="utf-8",
            )

            service = IngestService()
            without_metadata = service.scan_folder(root)
            with_metadata = service.scan_folder(root, collect_available_metadata=True)

            self.assertEqual(without_metadata[0].raw_import_data, "")
            self.assertIn("<metadata>", with_metadata[0].raw_import_data)
            self.assertIn("Road points", with_metadata[0].raw_import_data)

    def test_scan_rechecks_when_associated_xml_metadata_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.shp")
            metadata_path = root / "points.shp.xml"
            metadata_path.write_text(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><metadata><idinfo><citation>Version A</citation></idinfo></metadata>",
                encoding="utf-8",
            )

            service = IngestService()
            first = service.scan_folder(root, collect_available_metadata=True)
            metadata_path.write_text(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><metadata><idinfo><citation>Version B</citation></idinfo></metadata>",
                encoding="utf-8",
            )
            second = service.scan_folder(root, first, collect_available_metadata=True)

            self.assertIn("Version A", first[0].raw_import_data)
            self.assertIn("Version B", second[0].raw_import_data)

    def test_scan_refreshes_source_style_flags_when_sidecar_is_added(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")

            service = IngestService()
            first = service.scan_folder(root)
            self.assertFalse(first[0].has_source_style)

            (root / "points.qml").write_text("<qml/>", encoding="utf-8")
            second = service.scan_folder(root, first)

            self.assertEqual(len(second), 1)
            self.assertTrue(second[0].has_source_style)
            self.assertIn("points.qml", second[0].source_style_summary)

    def test_workspace_uses_data_out_and_ignores_internal_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = ensure_workspace(root)
            self.assertEqual(workspace.workspace_path.name, "data_out")
            self.assertTrue(workspace.temp_dir.exists())

            points = gpd.GeoDataFrame(
                {"name": ["a"]},
                geometry=[Point(10.4, 63.4)],
                crs="EPSG:4326",
            )
            points.to_file(root / "points.geojson", driver="GeoJSON")
            points.to_file(workspace.exports_dir / "exported.geojson", driver="GeoJSON")

            files = iter_supported_files(root)
            self.assertEqual([path.name for path in files], ["points.geojson"])


if __name__ == "__main__":
    unittest.main()

