from __future__ import annotations

from contextlib import closing
import gc
import os
from pathlib import Path
import sqlite3
import zipfile
from xml.etree import ElementTree as ET

import geopandas as gpd
import pandas as pd

from grasp.catalog.repository import CatalogRepository
from grasp.ingest.service import IngestService
from grasp.models import DatasetRecord, LayerStyle
from grasp.styling import StyleService, merge_bounds
from grasp.workspace import ProjectWorkspace, sanitize_layer_name

QGIS_TEMPLATE_QGZ_PATH = Path(__file__).resolve().parents[3] / "data" / "qgis" / "template.qgz"


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
            style_qml = self.style_service.qgis_style_qml(dataset, style)
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
                    "style_qml": style_qml,
                }
            )
            if dataset.bbox_wgs84:
                all_bounds.append(dataset.bbox_wgs84)
            del gdf
            gc.collect()
        project_dir = self._project_output_dir(target)
        project_xml = self.style_service.qgis_project_xml(
            project_name=target.stem,
            data_source=self._relative_project_data_source(project_dir, target),
            layer_specs=[
                {
                    "dataset_id": str(spec["dataset_id"]),
                    "display_name": str(spec["display_name"]),
                    "description": str(spec["description"]),
                    "layer_name": str(spec["layer_name"]),
                    "geometry_type": _geometry_type_for_qgis(str(spec["geometry_type"])),
                    "style_summary": cast_style(spec["style"]).summary,
                    "style_theme": cast_style(spec["style"]).theme,
                    "style_qml": str(spec.get("style_qml", "")),
                }
                for spec in layer_specs
            ],
            bounds=merge_bounds(all_bounds),
        )
        project_xml = self._project_xml_from_template(
            gpkg_path=target,
            project_dir=project_dir,
            project_name=target.stem,
            generated_project_xml=project_xml,
        )
        self._write_metadata_tables(target, datasets, layer_specs, project_xml)
        self._write_qgis_project_files(target, project_xml)
        gc.collect()
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

    def _write_qgis_project_files(self, gpkg_path: Path, project_xml: str) -> None:
        qgs_path, qgz_path = self._project_output_paths(gpkg_path)
        qgs_path.parent.mkdir(parents=True, exist_ok=True)
        qgs_path.write_text(project_xml, encoding="utf-8")
        with zipfile.ZipFile(qgz_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            copied_template_content = False
            if QGIS_TEMPLATE_QGZ_PATH.exists():
                with zipfile.ZipFile(QGIS_TEMPLATE_QGZ_PATH) as template_archive:
                    for info in template_archive.infolist():
                        if info.filename.lower().endswith(".qgs"):
                            archive.writestr(qgs_path.name, project_xml)
                        else:
                            archive.writestr(info, template_archive.read(info.filename))
                    copied_template_content = True
            if not copied_template_content:
                archive.writestr(qgs_path.name, project_xml)

    def _project_xml_from_template(
        self,
        *,
        gpkg_path: Path,
        project_dir: Path,
        project_name: str,
        generated_project_xml: str,
    ) -> str:
        if not QGIS_TEMPLATE_QGZ_PATH.exists():
            return generated_project_xml
        try:
            with zipfile.ZipFile(QGIS_TEMPLATE_QGZ_PATH) as archive:
                template_qgs_name = next(
                    name for name in archive.namelist() if name.lower().endswith(".qgs")
                )
                template_root = ET.fromstring(archive.read(template_qgs_name))
        except Exception:
            return generated_project_xml

        generated_root = ET.fromstring(generated_project_xml)
        template_root.set("projectname", project_name)
        title = template_root.find("title")
        if title is None:
            title = ET.SubElement(template_root, "title")
        title.text = project_name

        template_layer_tree = template_root.find("layer-tree-group")
        generated_layer_tree = generated_root.find("layer-tree-group")
        if template_layer_tree is not None and generated_layer_tree is not None:
            custom_order = template_layer_tree.find("custom-order")
            for layer_node in generated_layer_tree.findall("layer-tree-layer"):
                template_layer_tree.append(layer_node)
                if custom_order is not None:
                    ET.SubElement(custom_order, "item").text = layer_node.get("id", "")

        template_project_layers = template_root.find("projectlayers")
        generated_project_layers = generated_root.find("projectlayers")
        if template_project_layers is None and generated_project_layers is not None:
            template_project_layers = ET.SubElement(template_root, "projectlayers")
        if template_project_layers is not None and generated_project_layers is not None:
            for maplayer_node in generated_project_layers.findall("maplayer"):
                template_project_layers.append(maplayer_node)

        relative_path = self._relative_project_data_source(project_dir, gpkg_path)
        generated_layer_ids = {
            layer_node.get("id", "")
            for layer_node in generated_layer_tree.findall("layer-tree-layer")
        } if generated_layer_tree is not None else set()
        for maplayer in template_root.findall(".//projectlayers/maplayer"):
            layer_id = (maplayer.findtext("id") or "").strip()
            if layer_id not in generated_layer_ids:
                continue
            datasource = maplayer.find("datasource")
            if datasource is None:
                continue
            value = (datasource.text or "").strip()
            if "|layername=" in value:
                layer_suffix = value.split("|layername=", 1)[1]
                datasource.text = f"{relative_path}|layername={layer_suffix}"
            else:
                datasource.text = relative_path

        return ET.tostring(template_root, encoding="unicode")

    def _project_output_dir(self, export_path: Path) -> Path:
        if QGIS_TEMPLATE_QGZ_PATH.exists():
            return QGIS_TEMPLATE_QGZ_PATH.parent
        return export_path.parent

    def _project_output_paths(self, export_path: Path) -> tuple[Path, Path]:
        project_dir = self._project_output_dir(export_path)
        suffix_label = export_path.suffix.lstrip(".").lower() or "project"
        base_name = f"{export_path.stem}_{suffix_label}"
        return project_dir / f"{base_name}.qgs", project_dir / f"{base_name}.qgz"

    def _relative_project_data_source(self, project_dir: Path, export_path: Path) -> str:
        try:
            return Path(os.path.relpath(export_path, start=project_dir)).as_posix()
        except ValueError:
            return export_path.resolve().as_posix()

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

