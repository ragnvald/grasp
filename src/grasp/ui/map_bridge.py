from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd

from grasp.catalog.repository import CatalogRepository
from grasp.ingest.service import IngestService
from grasp.models import DatasetRecord, LayerStyle
from grasp.qt_compat import QObject, Signal, Slot
from grasp.styling import StyleService, merge_bounds
from grasp.workspace import ProjectWorkspace


MAP_PREVIEW_FEATURE_LIMITS = {
    "point": 2500,
    "line": 1500,
    "polygon": 900,
    "other": 1200,
}


class MapBridge(QObject):
    stateChanged = Signal(str)

    def __init__(self, workspace: ProjectWorkspace, repository: CatalogRepository) -> None:
        super().__init__()
        self.workspace = workspace
        self.repository = repository
        self.ingest_service = IngestService(workspace)
        self.style_service = StyleService()
        self._geojson_cache: dict[str, tuple[str, str]] = {}

    @Slot(result=str)
    def getState(self) -> str:
        groups = dict(self.repository.list_groups())
        datasets = []
        all_bounds = []
        for dataset in self.repository.list_datasets():
            if dataset.bbox_wgs84:
                all_bounds.append(dataset.bbox_wgs84)
            sources = self.repository.list_sources(dataset.dataset_id)
            selected_source = next((item for item in sources if item.is_selected), sources[0] if sources else None)
            geometry_category = _geometry_category(dataset.geometry_type)
            style = self._style_for_dataset(dataset, group_name=groups.get(dataset.group_id, dataset.group_id))
            datasets.append(
                {
                    "dataset_id": dataset.dataset_id,
                    "name": dataset.preferred_name,
                    "description": dataset.preferred_description,
                    "group_name": groups.get(dataset.group_id, dataset.group_id),
                    "geometry_type": dataset.geometry_type,
                    "geometry_category": geometry_category,
                    "visible": dataset.visibility,
                    "sort_order": dataset.sort_order,
                    "bbox": dataset.bbox_wgs84,
                    "color": style.stroke_color,
                    "style": style.to_map_payload(),
                    "source_url": selected_source.url if selected_source else "",
                    "cache_token": self._cache_token(dataset),
                }
            )
        payload = {
            "datasets": datasets,
            "bounds": merge_bounds(all_bounds),
        }
        return json.dumps(payload, ensure_ascii=False)

    @Slot(str, result=str)
    def getLayerGeoJson(self, dataset_id: str) -> str:
        dataset = self.repository.get_dataset(dataset_id)
        if not dataset:
            return json.dumps({"type": "FeatureCollection", "features": []})
        path = self._resolve_cache_path(dataset)
        if not path.exists():
            path = self.ingest_service.ensure_dataset_cache(dataset)
        cache_token = self._cache_token(dataset, path)
        cached = self._geojson_cache.get(dataset_id)
        if cached and cached[0] == cache_token:
            return cached[1]
        gdf = gpd.read_parquet(path)
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326, allow_override=True)
        elif str(gdf.crs).upper() != "EPSG:4326":
            gdf = gdf.to_crs(epsg=4326)
        gdf = _prepare_preview_gdf(gdf, _geometry_category(dataset.geometry_type))
        try:
            geojson = gdf.to_json(drop_id=True)
        except TypeError:
            geojson = gdf.to_json()
        self._geojson_cache[dataset_id] = (cache_token, geojson)
        return geojson

    def publish_state(self) -> None:
        self.stateChanged.emit(self.getState())

    def _resolve_cache_path(self, dataset) -> Path:
        return self.workspace.resolve_cache_path(dataset.dataset_id, dataset.cache_path)

    def _cache_token(self, dataset, path: Path | None = None) -> str:
        resolved = path or self._resolve_cache_path(dataset)
        parts = [dataset.dataset_id, dataset.cache_path or "", dataset.fingerprint or ""]
        if resolved.exists():
            stat = resolved.stat()
            parts.extend([str(stat.st_mtime_ns), str(stat.st_size)])
        return "|".join(parts)

    def _style_for_dataset(self, dataset: DatasetRecord, *, group_name: str) -> LayerStyle:
        stored = self.repository.get_style(dataset.dataset_id)
        if stored is not None:
            return stored
        return self.style_service.style_for_dataset(dataset, group_name=group_name)


def _geometry_category(geometry_type: str) -> str:
    value = str(geometry_type or "").lower()
    if "point" in value:
        return "point"
    if "line" in value:
        return "line"
    if "polygon" in value:
        return "polygon"
    return "other"


def _prepare_preview_gdf(gdf: gpd.GeoDataFrame, geometry_category: str) -> gpd.GeoDataFrame:
    if gdf.empty:
        return gdf
    out = gdf
    feature_limit = MAP_PREVIEW_FEATURE_LIMITS.get(geometry_category, MAP_PREVIEW_FEATURE_LIMITS["other"])
    if len(out) > feature_limit:
        step = max(1, -(-len(out) // feature_limit))
        out = out.iloc[::step].copy()
        if len(out) > feature_limit:
            out = out.head(feature_limit).copy()
    if geometry_category in {"line", "polygon"} and not out.empty:
        tolerance = _preview_simplification_tolerance(out.total_bounds.tolist(), geometry_category)
        if tolerance > 0:
            try:
                out["geometry"] = out.geometry.simplify(
                    tolerance,
                    preserve_topology=geometry_category == "polygon",
                )
                out = out[out.geometry.notna()].copy()
                out = out[~out.geometry.is_empty].copy()
            except Exception:
                pass
    return out


def _preview_simplification_tolerance(bounds: list[float], geometry_category: str) -> float:
    if not bounds or len(bounds) != 4:
        return 0.0
    minx, miny, maxx, maxy = (float(value) for value in bounds)
    span = max(abs(maxx - minx), abs(maxy - miny))
    if span <= 0:
        return 0.0
    if geometry_category == "polygon":
        return max(span / 1500.0, 0.00005)
    if geometry_category == "line":
        return max(span / 2500.0, 0.00002)
    return 0.0

