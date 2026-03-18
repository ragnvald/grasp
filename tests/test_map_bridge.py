from __future__ import annotations

import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point, Polygon

from grasp.catalog.repository import CatalogRepository
from grasp.models import DatasetRecord
from grasp.ui.map_bridge import (
    MAP_PREVIEW_FEATURE_LIMITS,
    MapBridge,
    _geometry_category,
    _prepare_preview_gdf,
    _preview_simplification_tolerance,
)
from grasp.workspace import ensure_workspace


class _FakeGeoDataFrame:
    def __init__(self) -> None:
        self.crs = None
        self.to_json_calls = 0
        self.empty = False

    def set_crs(self, epsg: int, allow_override: bool = False):  # noqa: ARG002
        self.crs = f"EPSG:{epsg}"
        return self

    def to_crs(self, epsg: int):
        self.crs = f"EPSG:{epsg}"
        return self

    def to_json(self, drop_id: bool = False) -> str:  # noqa: ARG002
        self.to_json_calls += 1
        return '{"type":"FeatureCollection","features":[]}'

    def __len__(self) -> int:
        return 1


class MapBridgeTests(unittest.TestCase):
    def test_geometry_category_detection(self) -> None:
        self.assertEqual(_geometry_category("Point"), "point")
        self.assertEqual(_geometry_category("MultiLineString"), "line")
        self.assertEqual(_geometry_category("Polygon"), "polygon")
        self.assertEqual(_geometry_category("GeometryCollection"), "other")

    def test_layer_geojson_is_cached_until_cache_token_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            workspace = ensure_workspace(tmp)
            repository = CatalogRepository(workspace.db_path)
            cache_path = workspace.dataset_cache_path("ds1")
            cache_path.write_text("placeholder", encoding="utf-8")
            dataset = DatasetRecord(
                dataset_id="ds1",
                source_path=str(Path(tmp) / "roads.geojson"),
                source_format="geojson",
                geometry_type="Point",
                feature_count=1,
                fingerprint="abc",
                cache_path=cache_path.relative_to(workspace.root_path).as_posix(),
            )
            repository.replace_datasets([dataset])
            bridge = MapBridge(workspace, repository)
            fake_gdf = _FakeGeoDataFrame()

            with patch("grasp.ui.map_bridge.gpd.read_parquet", return_value=fake_gdf) as read_parquet:
                first = bridge.getLayerGeoJson("ds1")
                second = bridge.getLayerGeoJson("ds1")

            self.assertEqual(first, second)
            self.assertEqual(read_parquet.call_count, 1)
            self.assertEqual(fake_gdf.to_json_calls, 1)

            state = json.loads(bridge.getState())
            self.assertTrue(state["datasets"][0]["cache_token"].startswith("ds1|"))
            self.assertIn("style", state["datasets"][0])
            self.assertTrue(state["datasets"][0]["style"]["theme"])

    def test_prepare_preview_gdf_limits_point_features(self) -> None:
        limit = MAP_PREVIEW_FEATURE_LIMITS["point"]
        gdf = gpd.GeoDataFrame(
            {"value": list(range(limit + 200))},
            geometry=[Point(float(index), 60.0) for index in range(limit + 200)],
            crs="EPSG:4326",
        )

        preview = _prepare_preview_gdf(gdf, "point")

        self.assertLessEqual(len(preview), limit)
        self.assertGreater(len(preview), 0)

    def test_prepare_preview_gdf_simplifies_polygon_geometry(self) -> None:
        ring = [
            (
                10.0 + math.cos((index / 200.0) * math.tau) * 0.1,
                63.0 + math.sin((index / 200.0) * math.tau) * 0.1,
            )
            for index in range(200)
        ]
        polygon = Polygon(ring + [ring[0]])
        gdf = gpd.GeoDataFrame({"value": [1]}, geometry=[polygon], crs="EPSG:4326")

        preview = _prepare_preview_gdf(gdf, "polygon")

        self.assertTrue(preview.geometry.iloc[0].is_valid)
        self.assertLessEqual(len(preview.geometry.iloc[0].exterior.coords), len(polygon.exterior.coords))

    def test_preview_simplification_tolerance_is_positive_for_lines(self) -> None:
        tolerance = _preview_simplification_tolerance([10.0, 63.0, 11.0, 64.0], "line")
        self.assertGreater(tolerance, 0.0)


if __name__ == "__main__":
    unittest.main()

