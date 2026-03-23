from __future__ import annotations

from contextlib import closing
from pathlib import Path
import re
import sqlite3


SOURCE_STYLE_FILE_LABELS = {
    ".qml": "QGIS QML style file",
    ".sld": "SLD style file",
    ".lyr": "ArcGIS layer file",
    ".lyrx": "ArcGIS Pro layer file",
    ".qlr": "QGIS layer definition",
    ".qgs": "QGIS project file",
    ".qgz": "QGIS project archive",
}
GPKG_STYLE_TABLE_LABELS = {
    "layer_styles": "GeoPackage layer_styles table",
    "qgis_layer_styles": "GeoPackage QGIS layer styles table",
    "gpkgext_styles": "GeoPackage styles extension table",
    "se_styles": "GeoPackage style table",
    "qgis_project": "GeoPackage QGIS project table",
    "qgis_projects": "GeoPackage QGIS project table",
}


def detect_source_style_evidence(file_path: str | Path, layer_name: str = "") -> list[dict[str, str]]:
    path = Path(file_path).expanduser().resolve()
    evidence = _detect_sidecar_style_files(path, layer_name)
    if path.suffix.lower() == ".gpkg":
        evidence.extend(_detect_gpkg_style_evidence(path, layer_name))
    return _deduplicate_style_evidence(evidence)


def summarize_source_style_evidence(evidence: list[dict[str, str]]) -> str:
    labels = [str(item.get("label", "")).strip() for item in evidence if str(item.get("label", "")).strip()]
    if not labels:
        return ""
    if len(labels) == 1:
        return f"Possible source styling detected: {labels[0]}."
    if len(labels) == 2:
        return f"Possible source styling detected: {labels[0]}; {labels[1]}."
    return f"Possible source styling detected: {labels[0]}; {labels[1]}; +{len(labels) - 2} more."


def describe_source_style_evidence(evidence: list[dict[str, str]]) -> str:
    labels = [str(item.get("label", "")).strip() for item in evidence if str(item.get("label", "")).strip()]
    return "\n".join(labels)


def _detect_sidecar_style_files(path: Path, layer_name: str) -> list[dict[str, str]]:
    if not path.parent.exists():
        return []
    base_tokens = {
        _style_match_token(path.stem),
        _style_match_token(path.name),
        _style_match_token(layer_name),
        _style_match_token(f"{path.stem}_{layer_name}"),
        _style_match_token(f"{path.name}_{layer_name}"),
    }
    base_tokens.discard("")
    evidence: list[dict[str, str]] = []
    for sibling in path.parent.iterdir():
        if not sibling.is_file():
            continue
        if sibling.resolve() == path:
            continue
        suffix = sibling.suffix.lower()
        label_prefix = SOURCE_STYLE_FILE_LABELS.get(suffix)
        if not label_prefix:
            continue
        sibling_token = _style_match_token(sibling.stem)
        if sibling_token not in base_tokens:
            continue
        evidence.append(
            {
                "kind": f"sidecar:{suffix.lstrip('.')}",
                "label": f"{label_prefix} ({sibling.name})",
                "path": str(sibling),
            }
        )
    return evidence


def _detect_gpkg_style_evidence(path: Path, layer_name: str) -> list[dict[str, str]]:
    evidence: list[dict[str, str]] = []
    try:
        with closing(sqlite3.connect(path)) as conn:
            tables = {
                str(row[0]).lower(): str(row[0])
                for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
            }
            for table_key, label in GPKG_STYLE_TABLE_LABELS.items():
                actual_table_name = tables.get(table_key)
                if not actual_table_name:
                    continue
                if table_key in {"layer_styles", "qgis_layer_styles"} and not _style_table_matches_layer(
                    conn,
                    actual_table_name,
                    layer_name,
                ):
                    continue
                evidence.append(
                    {
                        "kind": f"gpkg:{table_key}",
                        "label": label,
                        "path": str(path),
                    }
                )
    except Exception:
        return evidence
    return evidence


def _style_table_matches_layer(conn: sqlite3.Connection, table_name: str, layer_name: str) -> bool:
    target = str(layer_name or "").strip().lower()
    try:
        column_rows = conn.execute(f"PRAGMA table_info({_quoted_identifier(table_name)})").fetchall()
    except Exception:
        return True
    columns = [str(row[1]) for row in column_rows if len(row) > 1]
    if not columns:
        return True
    candidate_columns = [
        column
        for column in columns
        if str(column).strip().lower() in {"f_table_name", "tablename", "table_name", "layer_name"}
    ]
    if not candidate_columns:
        return True
    column_name = candidate_columns[0]
    try:
        rows = conn.execute(
            f"SELECT {_quoted_identifier(column_name)} FROM {_quoted_identifier(table_name)}"
        ).fetchall()
    except Exception:
        return True
    if not rows:
        return False
    if not target:
        return True
    return any(str(row[0] or "").strip().lower() == target for row in rows)


def _deduplicate_style_evidence(evidence: list[dict[str, str]]) -> list[dict[str, str]]:
    unique: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in evidence:
        key = (
            str(item.get("kind", "")).strip().lower(),
            str(item.get("label", "")).strip(),
            str(item.get("path", "")).strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    return unique


def _style_match_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'
