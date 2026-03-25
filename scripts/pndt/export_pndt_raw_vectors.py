from __future__ import annotations

import json
import re
import sqlite3
import sys
import unicodedata
from dataclasses import dataclass, asdict
from pathlib import Path

import geopandas as gpd


SOURCE_ROOT = Path(r"D:\utforsk\PNDT_entrega\Camadas")
OUTPUT_DIR = Path(r"D:\utforsk\PNDT_entrega_output")
OUTPUT_GPKG = OUTPUT_DIR / "pndt_vectors.gpkg"
MANIFEST_JSON = OUTPUT_DIR / "pndt_export_manifest.json"
MANIFEST_CSV = OUTPUT_DIR / "pndt_export_manifest.csv"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(errors="backslashreplace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(errors="backslashreplace")

ENCODING_FALLBACKS = [None, "utf-8", "latin1", "cp1252", "ISO-8859-1", "utf-8-sig"]


@dataclass
class LayerRecord:
    layer_name: str
    source_path: str
    relative_path: str
    feature_count: int
    geometry_type: str
    crs: str
    field_count: int
    status: str
    error: str = ""


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_value = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_value = ascii_value.lower()
    ascii_value = re.sub(r"[^a-z0-9]+", "_", ascii_value)
    ascii_value = re.sub(r"_+", "_", ascii_value).strip("_")
    return ascii_value or "layer"


def make_layer_name(relative_path: Path, used: set[str]) -> str:
    parts = [slugify(part) for part in relative_path.with_suffix("").parts]
    base = "__".join(parts)
    candidate = base
    suffix = 2
    while candidate in used:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used.add(candidate)
    return candidate


def write_manifest_tables(gpkg_path: Path, records: list[LayerRecord]) -> None:
    conn = sqlite3.connect(gpkg_path)
    try:
        conn.execute("DROP TABLE IF EXISTS pndt_layer_inventory")
        conn.execute(
            """
            CREATE TABLE pndt_layer_inventory (
                layer_name TEXT PRIMARY KEY,
                source_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                feature_count INTEGER NOT NULL,
                geometry_type TEXT,
                crs TEXT,
                field_count INTEGER NOT NULL,
                status TEXT NOT NULL,
                error TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO pndt_layer_inventory (
                layer_name,
                source_path,
                relative_path,
                feature_count,
                geometry_type,
                crs,
                field_count,
                status,
                error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    record.layer_name,
                    record.source_path,
                    record.relative_path,
                    record.feature_count,
                    record.geometry_type,
                    record.crs,
                    record.field_count,
                    record.status,
                    record.error,
                )
                for record in records
            ],
        )
        conn.commit()
    finally:
        conn.close()


def write_sidecar_manifests(records: list[LayerRecord]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_JSON.write_text(
        json.dumps([asdict(record) for record in records], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    lines = [
        "layer_name,source_path,relative_path,feature_count,geometry_type,crs,field_count,status,error"
    ]
    for record in records:
        values = [
            record.layer_name,
            record.source_path,
            record.relative_path,
            str(record.feature_count),
            record.geometry_type,
            record.crs,
            str(record.field_count),
            record.status,
            record.error,
        ]
        escaped = ['"' + value.replace('"', '""') + '"' for value in values]
        lines.append(",".join(escaped))
    MANIFEST_CSV.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_shapefile_with_fallback(path: Path) -> gpd.GeoDataFrame:
    last_error: Exception | None = None
    for encoding in ENCODING_FALLBACKS:
        try:
            if encoding is None:
                return gpd.read_file(path)
            return gpd.read_file(path, encoding=encoding)
        except Exception as exc:  # pragma: no cover - operational fallback
            last_error = exc
    assert last_error is not None
    raise last_error


def export() -> int:
    if not SOURCE_ROOT.exists():
        print(f"Source root not found: {SOURCE_ROOT}", file=sys.stderr)
        return 1

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    if OUTPUT_GPKG.exists():
        OUTPUT_GPKG.unlink()

    shapefiles = sorted(SOURCE_ROOT.rglob("*.shp"))
    used_names: set[str] = set()
    records: list[LayerRecord] = []

    print(f"Found {len(shapefiles)} shapefiles under {SOURCE_ROOT}")

    for index, shp_path in enumerate(shapefiles, start=1):
        relative_path = shp_path.relative_to(SOURCE_ROOT)
        layer_name = make_layer_name(relative_path, used_names)
        print(f"[{index}/{len(shapefiles)}] {relative_path} -> {layer_name}")

        try:
            gdf = read_shapefile_with_fallback(shp_path)
            feature_count = len(gdf)
            geometry_type = str(gdf.geom_type.dropna().mode().iloc[0]) if feature_count and not gdf.geom_type.dropna().empty else ""
            crs = gdf.crs.to_string() if gdf.crs else ""
            field_count = len(gdf.columns) - (1 if gdf.geometry.name in gdf.columns else 0)

            gdf.to_file(
                OUTPUT_GPKG,
                layer=layer_name,
                driver="GPKG",
                engine="pyogrio",
            )

            records.append(
                LayerRecord(
                    layer_name=layer_name,
                    source_path=str(shp_path),
                    relative_path=str(relative_path),
                    feature_count=feature_count,
                    geometry_type=geometry_type,
                    crs=crs,
                    field_count=field_count,
                    status="exported",
                )
            )
        except Exception as exc:  # pragma: no cover - operational export
            records.append(
                LayerRecord(
                    layer_name=layer_name,
                    source_path=str(shp_path),
                    relative_path=str(relative_path),
                    feature_count=0,
                    geometry_type="",
                    crs="",
                    field_count=0,
                    status="failed",
                    error=str(exc),
                )
            )
            print(f"  FAILED: {exc}", file=sys.stderr)

    exported = [record for record in records if record.status == "exported"]
    if OUTPUT_GPKG.exists():
        write_manifest_tables(OUTPUT_GPKG, records)
    write_sidecar_manifests(records)

    print("")
    print(f"GeoPackage: {OUTPUT_GPKG}")
    print(f"Manifest JSON: {MANIFEST_JSON}")
    print(f"Manifest CSV: {MANIFEST_CSV}")
    print(f"Exported layers: {len(exported)}")
    print(f"Failed layers: {len(records) - len(exported)}")

    return 0 if exported else 1


if __name__ == "__main__":
    raise SystemExit(export())
