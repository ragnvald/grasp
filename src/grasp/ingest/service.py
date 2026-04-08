from __future__ import annotations

from dataclasses import dataclass, field
import gc
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Callable

import fiona
import geopandas as gpd
from shapely.geometry import box as shapely_box
from shapely.geometry import shape as shapely_shape
from shapely import wkb as shapely_wkb

try:
    from shapely import force_2d as shapely_force_2d
except Exception:
    shapely_force_2d = None

try:
    from shapely.validation import make_valid
except Exception:
    make_valid = None

from grasp.models import DatasetRecord
from grasp.source_style import detect_source_style_evidence, summarize_source_style_evidence
from grasp.workspace import ProjectWorkspace, ensure_workspace, iter_supported_files, make_dataset_id


StatusCallback = Callable[[str], None]
ProgressCallback = Callable[[int], None]

PROFILE_SAMPLE_SIZE = 5
GEOMETRY_SAMPLE_SIZE = 3
MAX_AUTO_VISIBLE_FEATURES = 5000
MAX_AUTO_VISIBLE_DATASETS = 12
GEOJSON_TYPES = {
    "feature",
    "featurecollection",
    "geometrycollection",
    "point",
    "multipoint",
    "linestring",
    "multilinestring",
    "polygon",
    "multipolygon",
}
GEOJSON_SNIFF_BYTES = 65536
WGS84_MIN_LONGITUDE = -180.0
WGS84_MAX_LONGITUDE = 180.0
WGS84_MIN_LATITUDE = -90.0
WGS84_MAX_LATITUDE = 90.0


@dataclass(slots=True)
class DatasetSummary:
    geometry_type: str
    feature_count: int
    crs: str
    bbox_wgs84: list[float]
    column_profile: dict
    geometry_samples: list[str] = field(default_factory=list)


class IngestService:
    def __init__(self, workspace: ProjectWorkspace | None = None) -> None:
        self.workspace = workspace

    def scan_folder(
        self,
        path: str | Path,
        existing_records: dict[str, DatasetRecord] | list[DatasetRecord] | None = None,
        *,
        collect_available_metadata: bool = False,
        status_callback: StatusCallback | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> list[DatasetRecord]:
        workspace = self.workspace or ensure_workspace(path)
        self.workspace = workspace
        existing_by_id = self._normalize_existing_records(existing_records)
        workspace.clear_temp_dir()
        files, skipped_json = self._filter_supported_files(iter_supported_files(workspace.root_path))
        if progress_callback:
            progress_callback(0)
        if skipped_json:
            self._emit(status_callback, f"Skipped {skipped_json} non-GeoJSON .json file(s)")
        datasets: list[DatasetRecord] = []
        total_units = self._count_scan_units(files)
        processed = 0
        for file_path in files:
            suffix = file_path.suffix.lower()
            if suffix == ".gpkg":
                try:
                    layers = fiona.listlayers(file_path)
                except Exception as exc:
                    self._emit(status_callback, f"Skipping {file_path.name}: failed to list GeoPackage layers ({exc})")
                    processed += 1
                    self._progress(progress_callback, processed, total_units)
                    continue
                for layer_name in layers:
                    label = f"{file_path.name}:{layer_name}"
                    self._emit(status_callback, f"Scanning {processed + 1}/{total_units}: {label}")
                    record = self._build_dataset_record(
                        workspace,
                        file_path,
                        layer_name,
                        len(datasets),
                        existing_record=existing_by_id.get(make_dataset_id(workspace.root_path, file_path, layer_name)),
                        collect_available_metadata=collect_available_metadata,
                        status_callback=status_callback,
                    )
                    if record is not None:
                        datasets.append(record)
                    processed += 1
                    self._progress(progress_callback, processed, total_units)
            else:
                self._emit(status_callback, f"Scanning {processed + 1}/{total_units}: {file_path.name}")
                record = self._build_dataset_record(
                    workspace,
                    file_path,
                    "",
                    len(datasets),
                    existing_record=existing_by_id.get(make_dataset_id(workspace.root_path, file_path, "")),
                    collect_available_metadata=collect_available_metadata,
                    status_callback=status_callback,
                )
                if record is not None:
                    datasets.append(record)
                processed += 1
                self._progress(progress_callback, processed, total_units)
        removed = workspace.cleanup_orphaned_cache_files({dataset.dataset_id for dataset in datasets})
        if removed:
            self._emit(status_callback, f"Removed {removed} stale cache file(s)")
        workspace.clear_temp_dir()
        if progress_callback:
            progress_callback(100)
        self._apply_default_visibility(datasets)
        source_style_count = sum(1 for dataset in datasets if dataset.has_source_style)
        if source_style_count:
            self._emit(
                status_callback,
                f"Detected possible source styling for {source_style_count} dataset(s). Review before generating AI styles.",
            )
        self._emit(status_callback, f"Scan complete: {len(datasets)} dataset(s)")
        return datasets

    def ensure_dataset_cache(
        self,
        dataset: DatasetRecord,
        *,
        status_callback: StatusCallback | None = None,
    ) -> Path:
        if self.workspace is None:
            raise ValueError("Workspace is required to build dataset cache.")
        cache_path = self._resolve_cache_path(dataset.dataset_id, dataset.cache_path)
        file_path = self._resolve_source_path(dataset.source_path)
        label = f"{file_path.name}:{dataset.layer_name}" if dataset.layer_name else file_path.name
        if cache_path.exists():
            cache_quality_issue = self._cache_quality_issue(cache_path)
            if cache_quality_issue is None:
                return cache_path
            self._emit(
                status_callback,
                f"Rebuilding cached preview for {label}: {cache_quality_issue}.",
            )
            try:
                cache_path.unlink()
            except FileNotFoundError:
                pass
        self._emit(status_callback, f"Loading full dataset {label}")
        gdf = self._read_dataset(file_path, dataset.layer_name)
        self._emit(status_callback, f"Normalizing {label} ({len(gdf)} features)")
        gdf = self._normalize_geodataframe(gdf)
        quality_issue = self._quality_issue_from_gdf(gdf)
        if quality_issue is not None:
            raise ValueError(f"{label} failed geometry quality check: {quality_issue}.")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._emit(status_callback, f"Caching {label}")
        temp_path = self.workspace.temp_path(f"{dataset.dataset_id}.parquet")
        if temp_path.exists():
            temp_path.unlink()
        try:
            gdf.to_parquet(temp_path, index=False)
            temp_path.replace(cache_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()
            del gdf
            gc.collect()
        return cache_path

    def _count_scan_units(self, files: list[Path]) -> int:
        total = 0
        for file_path in files:
            if file_path.suffix.lower() == ".gpkg":
                try:
                    total += max(1, len(fiona.listlayers(file_path)))
                except Exception:
                    total += 1
            else:
                total += 1
        return max(total, 1)

    def _progress(self, callback: ProgressCallback | None, current: int, total: int) -> None:
        if callback:
            callback(int((current / max(total, 1)) * 100))

    def _emit(self, callback: StatusCallback | None, message: str) -> None:
        if callback:
            callback(message)

    def _build_dataset_record(
        self,
        workspace: ProjectWorkspace,
        file_path: Path,
        layer_name: str,
        sort_order: int,
        *,
        existing_record: DatasetRecord | None = None,
        collect_available_metadata: bool = False,
        status_callback: StatusCallback | None = None,
    ) -> DatasetRecord | None:
        label = f"{file_path.name}:{layer_name}" if layer_name else file_path.name
        dataset_id = make_dataset_id(workspace.root_path, file_path, layer_name)
        source_mtime_ns, source_size_bytes = self._source_signature(file_path)
        source_style_items = detect_source_style_evidence(file_path, layer_name)
        source_style_summary = summarize_source_style_evidence(source_style_items)
        source_style_items_json = json.dumps(source_style_items, ensure_ascii=False)
        raw_import_data = self._collect_associated_metadata(file_path) if collect_available_metadata else ""
        if existing_record is not None and self._can_reuse_existing_record(
            existing_record,
            source_mtime_ns,
            source_size_bytes,
            raw_import_data,
        ):
            cached_quality_issue = self._quality_issue_from_feature_count_and_bounds(
                existing_record.feature_count,
                existing_record.bbox_wgs84,
            )
            if cached_quality_issue is None:
                self._emit(status_callback, f"Reusing existing catalog metadata for {label}")
                return self._reuse_existing_record(
                    workspace,
                    existing_record,
                    file_path,
                    sort_order,
                    source_mtime_ns,
                    source_size_bytes,
                    source_style_summary,
                    source_style_items_json,
                    raw_import_data,
                )
            self._emit(
                status_callback,
                f"Rechecking {label}: stored metadata failed geometry quality check ({cached_quality_issue}).",
            )
        try:
            summary = self._summarize_dataset(file_path, layer_name, status_callback=status_callback)
        except Exception as exc:
            return self._skip(workspace, file_path, layer_name, exc, status_callback=status_callback)
        quality_issue = self._quality_issue_from_feature_count_and_bounds(summary.feature_count, summary.bbox_wgs84)
        if quality_issue is not None:
            return self._skip(
                workspace,
                file_path,
                layer_name,
                ValueError(f"failed geometry quality check: {quality_issue}"),
                status_callback=status_callback,
            )
        self._invalidate_existing_cache(workspace, dataset_id, existing_record)
        cache_path = workspace.dataset_cache_path(dataset_id)
        return DatasetRecord(
            dataset_id=dataset_id,
            source_path=str(file_path),
            source_format=file_path.suffix.lower().lstrip("."),
            source_mtime_ns=source_mtime_ns,
            source_size_bytes=source_size_bytes,
            layer_name=layer_name,
            geometry_type=summary.geometry_type,
            feature_count=summary.feature_count,
            crs=summary.crs,
            bbox_wgs84=summary.bbox_wgs84,
            column_profile_json=json.dumps(summary.column_profile, ensure_ascii=False),
            fingerprint=self._fingerprint_from_summary(summary),
            sort_order=sort_order,
            visibility=summary.feature_count <= MAX_AUTO_VISIBLE_FEATURES,
            raw_import_data=raw_import_data,
            source_style_summary=source_style_summary,
            source_style_items_json=source_style_items_json,
            cache_path=str(cache_path),
        )

    def _skip(
        self,
        workspace: ProjectWorkspace,
        file_path: Path,
        layer_name: str,
        exc: Exception,
        *,
        status_callback: StatusCallback | None = None,
    ) -> None:
        label = f"{file_path.name}:{layer_name}" if layer_name else file_path.name
        self._emit(status_callback, f"Skipping {label}: {exc}")
        log_path = workspace.log_path("ingest.log")
        with log_path.open("a", encoding="utf-8") as handle:
            layer_note = f":{layer_name}" if layer_name else ""
            handle.write(f"Failed reading {file_path}{layer_note}: {exc}\n")
        return None

    def _read_dataset(self, file_path: Path, layer_name: str) -> gpd.GeoDataFrame:
        suffix = file_path.suffix.lower()
        if suffix == ".parquet":
            return gpd.read_parquet(file_path)
        if suffix == ".json" and not self._is_geojson_file(file_path):
            raise ValueError(f"{file_path} is not a GeoJSON file.")
        if suffix in {".geojson", ".json", ".shp"}:
            return gpd.read_file(file_path)
        if suffix == ".gpkg":
            return gpd.read_file(file_path, layer=layer_name or None)
        raise ValueError(f"Unsupported suffix: {suffix}")

    def _normalize_geodataframe(self, gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        out = gdf
        if "geometry" not in out.columns:
            out["geometry"] = None
            out = gpd.GeoDataFrame(out, geometry="geometry", crs="EPSG:4326")
        if out.crs is None:
            out = out.set_crs(epsg=4326, allow_override=True)
        elif str(out.crs).upper() != "EPSG:4326":
            out = out.to_crs(epsg=4326)
        out = out[out.geometry.notna()].copy()
        if not out.empty:
            out["geometry"] = out.geometry.map(self._normalize_geometry)
            out = out[out.geometry.notna()].copy()
        return out

    def _normalize_geometry(self, geometry):
        if geometry is None:
            return None
        try:
            if geometry.is_empty:
                return geometry
        except Exception:
            return None
        if make_valid is not None:
            try:
                geometry = make_valid(geometry)
            except Exception:
                pass
        else:
            try:
                geometry = geometry.buffer(0)
            except Exception:
                pass
        if shapely_force_2d is not None:
            try:
                return shapely_force_2d(geometry)
            except Exception:
                pass
        try:
            return shapely_wkb.loads(shapely_wkb.dumps(geometry, output_dimension=2))
        except Exception:
            return geometry

    def _geometry_type(self, gdf: gpd.GeoDataFrame) -> str:
        if gdf.empty:
            return "Unknown"
        types = sorted({str(value) for value in gdf.geometry.geom_type.dropna().unique().tolist()})
        return ", ".join(types) if types else "Unknown"

    def _filter_supported_files(self, files: list[Path]) -> tuple[list[Path], int]:
        kept: list[Path] = []
        skipped_json = 0
        for file_path in files:
            if file_path.suffix.lower() == ".json" and not self._is_geojson_file(file_path):
                skipped_json += 1
                continue
            kept.append(file_path)
        return kept, skipped_json

    def _apply_default_visibility(self, datasets: list[DatasetRecord]) -> None:
        visible_datasets = [dataset for dataset in datasets if dataset.visibility]
        for dataset in visible_datasets[MAX_AUTO_VISIBLE_DATASETS:]:
            dataset.visibility = False

    def _summarize_dataset(
        self,
        file_path: Path,
        layer_name: str,
        *,
        status_callback: StatusCallback | None = None,
    ) -> DatasetSummary:
        suffix = file_path.suffix.lower()
        if suffix == ".parquet":
            gdf = self._read_dataset(file_path, layer_name)
            gdf = self._normalize_geodataframe(gdf)
            profile = self._column_profile(gdf, file_path, layer_name)
            return DatasetSummary(
                geometry_type=self._geometry_type(gdf),
                feature_count=int(len(gdf)),
                crs=str(gdf.crs) if gdf.crs else "EPSG:4326",
                bbox_wgs84=self._bbox_wgs84(gdf),
                column_profile=profile,
                geometry_samples=self._geometry_sample_hashes_from_gdf(gdf),
            )
        label = f"{file_path.name}:{layer_name}" if layer_name else file_path.name
        self._emit(status_callback, f"Inspecting metadata for {label}")
        return self._summarize_vector_dataset(file_path, layer_name)

    def _summarize_vector_dataset(self, file_path: Path, layer_name: str) -> DatasetSummary:
        with fiona.open(file_path, layer=layer_name or None) as src:
            crs_text = self._fiona_crs_text(src)
            column_profile, geometry_samples = self._sample_vector_profile(src, file_path, layer_name)
            return DatasetSummary(
                geometry_type=self._schema_geometry_type(src.schema.get("geometry")),
                feature_count=int(len(src)),
                crs=crs_text,
                bbox_wgs84=self._bounds_to_wgs84(src.bounds, crs_text),
                column_profile=column_profile,
                geometry_samples=geometry_samples,
            )

    def _sample_vector_profile(
        self,
        src,
        file_path: Path,
        layer_name: str,
    ) -> tuple[dict, list[str]]:
        properties = src.schema.get("properties") or {}
        sample_values = {name: [] for name in properties}
        seen_values = {name: set() for name in properties}
        geometry_samples: list[str] = []
        for index, feature in enumerate(src):
            if index >= PROFILE_SAMPLE_SIZE:
                break
            feature_props = dict(feature.get("properties") or {})
            for name in properties:
                value = feature_props.get(name)
                if value is None:
                    continue
                text = str(value).strip()
                if not text or text in seen_values[name]:
                    continue
                seen_values[name].add(text)
                sample_values[name].append(text)
            geometry = feature.get("geometry")
            if geometry is None or len(geometry_samples) >= GEOMETRY_SAMPLE_SIZE:
                continue
            try:
                geometry_samples.append(hashlib.sha1(shapely_shape(geometry).wkb).hexdigest()[:12])
            except Exception:
                geometry_samples.append("none")
        columns = [
            {
                "name": name,
                "dtype": str(dtype),
                "null_count": None,
                "unique_non_null": len(seen_values[name]),
                "samples": sample_values[name][:PROFILE_SAMPLE_SIZE],
            }
            for name, dtype in properties.items()
        ]
        return (
            {
                "file_name": file_path.name,
                "layer_name": layer_name,
                "column_count": len(columns),
                "profile_mode": "metadata-sample",
                "columns": columns,
            },
            geometry_samples,
        )

    def _fiona_crs_text(self, src) -> str:
        crs_wkt = getattr(src, "crs_wkt", "")
        if crs_wkt:
            return str(crs_wkt)
        crs = getattr(src, "crs", None)
        if crs:
            try:
                return str(fiona.crs.to_string(crs))
            except Exception:
                return str(crs)
        return "EPSG:4326"

    def _schema_geometry_type(self, geometry_name: object) -> str:
        text = str(geometry_name or "").strip()
        return text or "Unknown"

    def _bounds_to_wgs84(self, bounds, crs_text: str) -> list[float]:
        if not bounds:
            return []
        try:
            geom = gpd.GeoSeries([shapely_box(*bounds)], crs=crs_text or "EPSG:4326")
            if str(geom.crs).upper() != "EPSG:4326":
                geom = geom.to_crs(epsg=4326)
            return [round(float(value), 8) for value in geom.total_bounds.tolist()]
        except Exception:
            return [round(float(value), 8) for value in list(bounds)]

    def _resolve_source_path(self, source_path: str) -> Path:
        path = Path(source_path)
        if path.is_absolute():
            return path
        if self.workspace is None:
            return path.resolve()
        return (self.workspace.root_path / path).resolve()

    def _resolve_cache_path(self, dataset_id: str, cache_path: str) -> Path:
        if self.workspace is None:
            raise ValueError("Workspace is required to resolve cache path.")
        return self.workspace.resolve_cache_path(dataset_id, cache_path)

    def _normalize_existing_records(
        self,
        existing_records: dict[str, DatasetRecord] | list[DatasetRecord] | None,
    ) -> dict[str, DatasetRecord]:
        if existing_records is None:
            return {}
        if isinstance(existing_records, dict):
            return {str(key): value for key, value in existing_records.items()}
        return {record.dataset_id: record for record in existing_records}

    def _source_signature(self, file_path: Path) -> tuple[int, int]:
        stat = file_path.stat()
        return int(stat.st_mtime_ns), int(stat.st_size)

    def _collect_associated_metadata(self, file_path: Path) -> str:
        blocks: list[str] = []
        metadata_paths = self._associated_metadata_paths(file_path)
        for metadata_path in metadata_paths:
            text = self._read_associated_metadata_text(metadata_path).strip()
            if not text:
                continue
            if len(metadata_paths) == 1:
                blocks.append(text)
            else:
                blocks.append(f"[{metadata_path.name}]\n{text}")
        return "\n\n".join(blocks).strip()

    def _associated_metadata_paths(self, file_path: Path) -> list[Path]:
        candidates = [
            file_path.parent / f"{file_path.name}.xml",
            file_path.with_suffix(".xml"),
        ]
        unique_paths: list[Path] = []
        seen: set[Path] = set()
        for candidate in candidates:
            resolved = candidate.resolve()
            if resolved in seen or not candidate.exists() or not candidate.is_file():
                continue
            seen.add(resolved)
            unique_paths.append(candidate)
        return unique_paths

    def _read_associated_metadata_text(self, path: Path) -> str:
        payload = path.read_bytes()
        for encoding in ("utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
            try:
                return payload.decode(encoding)
            except UnicodeDecodeError:
                continue
        return payload.decode("latin-1", errors="replace")

    def _can_reuse_existing_record(
        self,
        existing_record: DatasetRecord,
        source_mtime_ns: int,
        source_size_bytes: int,
        raw_import_data: str,
    ) -> bool:
        return (
            int(existing_record.source_mtime_ns or 0) == source_mtime_ns
            and int(existing_record.source_size_bytes or 0) == source_size_bytes
            and str(existing_record.raw_import_data or "") == str(raw_import_data or "")
        )

    def _reuse_existing_record(
        self,
        workspace: ProjectWorkspace,
        existing_record: DatasetRecord,
        file_path: Path,
        sort_order: int,
        source_mtime_ns: int,
        source_size_bytes: int,
        source_style_summary: str,
        source_style_items_json: str,
        raw_import_data: str,
    ) -> DatasetRecord:
        migrated_cache_path = str(workspace.dataset_cache_path(existing_record.dataset_id))
        return DatasetRecord(
            dataset_id=existing_record.dataset_id,
            source_path=str(file_path),
            source_format=file_path.suffix.lower().lstrip("."),
            source_mtime_ns=source_mtime_ns,
            source_size_bytes=source_size_bytes,
            layer_name=existing_record.layer_name,
            display_name_user=existing_record.display_name_user,
            display_name_ai=existing_record.display_name_ai,
            description_user=existing_record.description_user,
            description_ai=existing_record.description_ai,
            raw_import_data=raw_import_data,
            geometry_type=existing_record.geometry_type,
            feature_count=existing_record.feature_count,
            crs=existing_record.crs,
            bbox_wgs84=list(existing_record.bbox_wgs84),
            column_profile_json=existing_record.column_profile_json,
            fingerprint=existing_record.fingerprint,
            group_id=existing_record.group_id,
            sort_order=existing_record.sort_order if existing_record.sort_order else sort_order,
            visibility=existing_record.visibility,
            include_in_export=existing_record.include_in_export,
            source_style_summary=source_style_summary,
            source_style_items_json=source_style_items_json,
            cache_path=migrated_cache_path,
            ai_confidence=existing_record.ai_confidence,
            suggested_group=existing_record.suggested_group,
        )

    def _invalidate_existing_cache(
        self,
        workspace: ProjectWorkspace,
        dataset_id: str,
        existing_record: DatasetRecord | None,
    ) -> None:
        candidates = {workspace.dataset_cache_path(dataset_id)}
        if existing_record and existing_record.cache_path:
            candidates.add(workspace.resolve_cache_path(dataset_id, existing_record.cache_path))
        for path in candidates:
            if not path.exists():
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue

    def _is_geojson_file(self, file_path: Path) -> bool:
        suffix = file_path.suffix.lower()
        if suffix == ".geojson":
            return True
        if suffix != ".json":
            return False
        try:
            with file_path.open("r", encoding="utf-8-sig") as handle:
                prefix = handle.read(GEOJSON_SNIFF_BYTES)
        except Exception:
            return False
        return self._looks_like_geojson_text(prefix)

    def _looks_like_geojson_text(self, value: str) -> bool:
        text = str(value or "").lstrip()
        if not text:
            return False
        lowered = text.lower()
        if lowered.startswith("{"):
            if '"features"' in lowered and "[" in lowered:
                return True
            if '"geometries"' in lowered and "[" in lowered:
                return True
            type_match = re.search(r'"type"\s*:\s*"([^"]+)"', lowered)
            if type_match and type_match.group(1) in GEOJSON_TYPES:
                return True
            return False
        if lowered.startswith("["):
            return '"type"' in lowered and '"feature"' in lowered
        return False

    def _bbox_wgs84(self, gdf: gpd.GeoDataFrame) -> list[float]:
        if gdf.empty:
            return []
        return [round(float(value), 8) for value in gdf.total_bounds.tolist()]

    def _cache_quality_issue(self, cache_path: Path) -> str | None:
        try:
            cached = gpd.read_parquet(cache_path)
        except Exception as exc:
            return f"cached preview could not be read ({exc})"
        try:
            cached = self._normalize_geodataframe(cached)
        except Exception as exc:
            return f"cached preview could not be normalized ({exc})"
        return self._quality_issue_from_gdf(cached)

    def _quality_issue_from_gdf(self, gdf: gpd.GeoDataFrame) -> str | None:
        return self._quality_issue_from_feature_count_and_bounds(len(gdf), self._bbox_wgs84(gdf))

    def _quality_issue_from_feature_count_and_bounds(self, feature_count: int, bbox_wgs84: list[float]) -> str | None:
        if int(feature_count or 0) <= 0:
            return "no usable geometries remain after validation"
        if not self._has_usable_wgs84_bounds(bbox_wgs84):
            if bbox_wgs84:
                return "bounds are outside usable WGS84 range; CRS is likely missing or incorrect"
            return "no usable geographic bounds were found"
        return None

    def _has_usable_wgs84_bounds(self, bounds: list[float]) -> bool:
        if len(bounds) != 4:
            return False
        try:
            minx, miny, maxx, maxy = (float(value) for value in bounds)
        except (TypeError, ValueError):
            return False
        if not all(math.isfinite(value) for value in (minx, miny, maxx, maxy)):
            return False
        if minx > maxx or miny > maxy:
            return False
        return (
            WGS84_MIN_LONGITUDE <= minx <= WGS84_MAX_LONGITUDE
            and WGS84_MIN_LONGITUDE <= maxx <= WGS84_MAX_LONGITUDE
            and WGS84_MIN_LATITUDE <= miny <= WGS84_MAX_LATITUDE
            and WGS84_MIN_LATITUDE <= maxy <= WGS84_MAX_LATITUDE
        )

    def _column_profile(self, gdf: gpd.GeoDataFrame, file_path: Path, layer_name: str) -> dict:
        columns: list[dict[str, object]] = []
        for name in gdf.columns:
            if name == gdf.geometry.name:
                continue
            series = gdf[name]
            samples = [str(value) for value in series.dropna().astype(str).head(5).tolist() if str(value).strip()]
            columns.append(
                {
                    "name": name,
                    "dtype": str(series.dtype),
                    "null_count": int(series.isna().sum()),
                    "unique_non_null": int(series.dropna().nunique()),
                    "samples": samples,
                }
            )
        return {
            "file_name": file_path.name,
            "layer_name": layer_name,
            "column_count": len(columns),
            "columns": columns,
        }

    def _geometry_sample_hashes_from_gdf(self, gdf: gpd.GeoDataFrame) -> list[str]:
        geometry_samples: list[str] = []
        if gdf.empty:
            return geometry_samples
        for geometry in gdf.geometry.head(GEOMETRY_SAMPLE_SIZE).tolist():
            try:
                geometry_samples.append(hashlib.sha1(geometry.wkb).hexdigest()[:12])
            except Exception:
                geometry_samples.append("none")
        return geometry_samples

    def _fingerprint(self, gdf: gpd.GeoDataFrame, profile: dict) -> str:
        return self._fingerprint_from_components(
            feature_count=int(len(gdf)),
            geometry_types=sorted(gdf.geometry.geom_type.dropna().astype(str).unique().tolist()) if not gdf.empty else [],
            bbox=self._bbox_wgs84(gdf),
            profile=profile,
            geometry_samples=self._geometry_sample_hashes_from_gdf(gdf),
        )

    def _fingerprint_from_summary(self, summary: DatasetSummary) -> str:
        return self._fingerprint_from_components(
            feature_count=summary.feature_count,
            geometry_types=[summary.geometry_type] if summary.geometry_type else [],
            bbox=summary.bbox_wgs84,
            profile=summary.column_profile,
            geometry_samples=summary.geometry_samples,
        )

    def _fingerprint_from_components(
        self,
        *,
        feature_count: int,
        geometry_types: list[str],
        bbox: list[float],
        profile: dict,
        geometry_samples: list[str],
    ) -> str:
        payload = {
            "feature_count": feature_count,
            "geometry_types": sorted(str(value).lower() for value in geometry_types),
            "bbox": bbox,
            "columns": [
                {
                    "name": column["name"],
                    "dtype": self._normalize_profile_dtype(str(column["dtype"])),
                    "samples": column["samples"][:3],
                }
                for column in profile.get("columns", [])
            ],
            "geometry_samples": geometry_samples,
        }
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def _normalize_profile_dtype(self, value: str) -> str:
        normalized = value.strip().lower()
        if normalized in {"str", "string", "object"}:
            return "text"
        if normalized in {"int", "int32", "int64", "integer"}:
            return "integer"
        if normalized in {"float", "float32", "float64", "real"}:
            return "float"
        if normalized in {"bool", "boolean"}:
            return "boolean"
        return normalized
