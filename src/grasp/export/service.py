from __future__ import annotations

from contextlib import closing
import gc
import json
from pathlib import Path
import sqlite3

import geopandas as gpd
import pandas as pd

from grasp.catalog.repository import CatalogRepository
from grasp.ingest.service import IngestService
from grasp.models import DatasetRecord, LayerStyle
from grasp.styling import StyleService, merge_bounds
from grasp.workspace import ProjectWorkspace, sanitize_layer_name


class ExportService:
    def __init__(self, workspace: ProjectWorkspace, repository: CatalogRepository) -> None:
        self.workspace = workspace
        self.repository = repository
        self.ingest_service = IngestService(workspace)
        self.style_service = StyleService()

    def export_gpkg(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        datasets = [dataset for dataset in self.repository.list_datasets() if dataset.include_in_export]
        group_lookup = dict(self.repository.list_groups())
        used_layer_names: set[str] = set()
        layer_specs: list[dict[str, object]] = []
        all_bounds: list[list[float]] = []
        for dataset in datasets:
            gdf = self._load_cache(dataset)
            layer_name = sanitize_layer_name(dataset.preferred_name or dataset.dataset_id, used_layer_names)
            used_layer_names.add(layer_name)
            geometry_column = str(gdf.geometry.name)
            gdf.to_file(target, layer=layer_name, driver="GPKG")
            style = self._style_for_dataset(dataset, group_name=group_lookup.get(dataset.group_id, dataset.group_id))
            layer_specs.append(
                {
                    "dataset_id": dataset.dataset_id,
                    "display_name": dataset.preferred_name,
                    "description": dataset.preferred_description,
                    "source_path": dataset.source_path,
                    "layer_name": layer_name,
                    "geometry_type": dataset.geometry_type,
                    "geometry_column": geometry_column,
                    "group_name": group_lookup.get(dataset.group_id, dataset.group_id),
                    "style": style,
                }
            )
            if dataset.bbox_wgs84:
                all_bounds.append(dataset.bbox_wgs84)
            del gdf
            gc.collect()
        project_xml = self.style_service.qgis_project_xml(
            project_name=target.stem,
            gpkg_path=target,
            layer_specs=[
                {
                    "dataset_id": str(spec["dataset_id"]),
                    "display_name": str(spec["display_name"]),
                    "description": str(spec["description"]),
                    "layer_name": str(spec["layer_name"]),
                    "geometry_type": _geometry_type_for_qgis(str(spec["geometry_type"])),
                    "style_summary": cast_style(spec["style"]).summary,
                    "style_theme": cast_style(spec["style"]).theme,
                }
                for spec in layer_specs
            ],
            bounds=merge_bounds(all_bounds),
        )
        self._write_metadata_tables(target, datasets, layer_specs, project_xml)
        target.with_suffix(".qgs").write_text(project_xml, encoding="utf-8")
        gc.collect()
        return target

    def export_geoparquet(self, path: str | Path) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        datasets = [dataset for dataset in self.repository.list_datasets() if dataset.include_in_export]
        rows: list[gpd.GeoDataFrame] = []
        selected_sources = {dataset.dataset_id: self._selected_source(dataset.dataset_id) for dataset in datasets}
        group_lookup = dict(self.repository.list_groups())
        for dataset in datasets:
            gdf = self._load_cache(dataset)
            if gdf.empty:
                continue
            source = selected_sources.get(dataset.dataset_id)
            out = gdf.copy()
            non_geometry_columns = [column for column in out.columns if column != out.geometry.name]
            out["dataset_id"] = dataset.dataset_id
            out["dataset_name"] = dataset.preferred_name
            out["group_name"] = group_lookup.get(dataset.group_id, dataset.group_id)
            out["description"] = dataset.preferred_description
            out["source_url"] = source.url if source else ""
            out["source_evidence_json"] = source.to_json() if source else ""
            out["attributes_json"] = out[non_geometry_columns].apply(
                lambda row: json.dumps({key: _coerce_value(value) for key, value in row.items()}, ensure_ascii=False),
                axis=1,
            )
            keep_columns = [
                "dataset_id",
                "dataset_name",
                "group_name",
                "description",
                "attributes_json",
                "source_url",
                "source_evidence_json",
                out.geometry.name,
            ]
            out = out[keep_columns]
            if out.geometry.name != "geometry":
                out = out.rename_geometry("geometry")
            rows.append(out)
        if rows:
            merged = gpd.GeoDataFrame(pd.concat(rows, ignore_index=True), geometry="geometry", crs="EPSG:4326")
        else:
            merged = gpd.GeoDataFrame(
                columns=[
                    "dataset_id",
                    "dataset_name",
                    "group_name",
                    "description",
                    "attributes_json",
                    "source_url",
                    "source_evidence_json",
                    "geometry",
                ],
                geometry="geometry",
                crs="EPSG:4326",
            )
        merged.to_parquet(target, index=False)
        return target

    def _write_metadata_tables(
        self,
        gpkg_path: Path,
        datasets: list[DatasetRecord],
        layer_specs: list[dict[str, object]],
        project_xml: str,
    ) -> None:
        group_lookup = dict(self.repository.list_groups())
        spec_lookup = {str(spec["dataset_id"]): spec for spec in layer_specs}
        dataset_rows = []
        source_rows = []
        style_rows = []
        for dataset in datasets:
            spec = spec_lookup.get(dataset.dataset_id, {})
            style = cast_style(spec.get("style"))
            dataset_rows.append(
                {
                    "dataset_id": dataset.dataset_id,
                    "source_path": dataset.source_path,
                    "layer_name": spec.get("layer_name", dataset.layer_name),
                    "display_name": dataset.preferred_name,
                    "description": dataset.preferred_description,
                    "group_name": group_lookup.get(dataset.group_id, dataset.group_id),
                    "geometry_type": dataset.geometry_type,
                    "feature_count": dataset.feature_count,
                    "crs": dataset.crs,
                    "fingerprint": dataset.fingerprint,
                    "style_label": style.label,
                    "style_theme": style.theme,
                    "style_summary": style.summary,
                    "style_json": style.to_json(),
                }
            )
            style_rows.append(
                {
                    "f_table_catalog": "",
                    "f_table_schema": "",
                    "f_table_name": spec.get("layer_name", dataset.layer_name),
                    "f_geometry_column": spec.get("geometry_column", "geometry"),
                    "styleName": style.label or dataset.preferred_name,
                    "styleQML": self.style_service.qgis_style_qml(dataset, style),
                    "styleSLD": "",
                    "useAsDefault": 1,
                    "description": style.summary,
                    "owner": "GRASP",
                    "ui": "",
                    "update_time": "",
                }
            )
            for source in self.repository.list_sources(dataset.dataset_id):
                source_rows.append(
                    {
                        "candidate_id": source.candidate_id,
                        "dataset_id": dataset.dataset_id,
                        "url": source.url,
                        "title": source.title,
                        "snippet": source.snippet,
                        "domain": source.domain,
                        "source_type": source.source_type,
                        "match_reason": source.match_reason,
                        "confidence": source.confidence,
                        "is_selected": int(source.is_selected),
                    }
                )
        with closing(sqlite3.connect(gpkg_path)) as conn:
            pd.DataFrame(
                dataset_rows,
                columns=[
                    "dataset_id",
                    "source_path",
                    "layer_name",
                    "display_name",
                    "description",
                    "group_name",
                    "geometry_type",
                    "feature_count",
                    "crs",
                    "fingerprint",
                    "style_label",
                    "style_theme",
                    "style_summary",
                    "style_json",
                ],
            ).to_sql("dataset_catalog", conn, if_exists="replace", index=False)
            pd.DataFrame(
                source_rows,
                columns=[
                    "candidate_id",
                    "dataset_id",
                    "url",
                    "title",
                    "snippet",
                    "domain",
                    "source_type",
                    "match_reason",
                    "confidence",
                    "is_selected",
                ],
            ).to_sql("source_candidates", conn, if_exists="replace", index=False)
            pd.DataFrame(
                style_rows,
                columns=[
                    "f_table_catalog",
                    "f_table_schema",
                    "f_table_name",
                    "f_geometry_column",
                    "styleName",
                    "styleQML",
                    "styleSLD",
                    "useAsDefault",
                    "description",
                    "owner",
                    "ui",
                    "update_time",
                ],
            ).to_sql("layer_styles", conn, if_exists="replace", index=False)
            pd.DataFrame(
                [
                    {
                        "project_name": gpkg_path.stem,
                        "project_xml": project_xml,
                    }
                ]
            ).to_sql("grasp_qgis_project", conn, if_exists="replace", index=False)
            conn.commit()

    def _selected_source(self, dataset_id: str):
        sources = self.repository.list_sources(dataset_id)
        for source in sources:
            if source.is_selected:
                return source
        return sources[0] if sources else None

    def _style_for_dataset(self, dataset: DatasetRecord, *, group_name: str) -> LayerStyle:
        stored = self.repository.get_style(dataset.dataset_id)
        if stored is not None:
            return stored
        return self.style_service.style_for_dataset(dataset, group_name=group_name)

    def _load_cache(self, dataset: DatasetRecord) -> gpd.GeoDataFrame:
        path = self.workspace.resolve_cache_path(dataset.dataset_id, dataset.cache_path)
        if not path.exists():
            path = self.ingest_service.ensure_dataset_cache(dataset)
        gdf = gpd.read_parquet(path)
        if gdf.crs is None:
            gdf = gdf.set_crs(epsg=4326, allow_override=True)
        elif str(gdf.crs).upper() != "EPSG:4326":
            gdf = gdf.to_crs(epsg=4326)
        return gdf


def _coerce_value(value):
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


def cast_style(value: object) -> LayerStyle:
    if isinstance(value, LayerStyle):
        return value
    return LayerStyle()


def _geometry_type_for_qgis(value: str) -> str:
    lowered = str(value or "").lower()
    if "point" in lowered:
        return "Point"
    if "line" in lowered:
        return "Line"
    if "polygon" in lowered:
        return "Polygon"
    return "Unknown"

