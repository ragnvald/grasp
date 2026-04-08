from __future__ import annotations

import json
import random
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import uuid

from grasp.models import DatasetRecord, DatasetUnderstanding, LayerStyle, SourceCandidate
from grasp.workspace import display_group_name, sanitize_group_id


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT PRIMARY KEY,
    source_path TEXT NOT NULL,
    source_format TEXT NOT NULL,
    source_mtime_ns INTEGER NOT NULL DEFAULT 0,
    source_size_bytes INTEGER NOT NULL DEFAULT 0,
    layer_name TEXT NOT NULL DEFAULT '',
    display_name_user TEXT NOT NULL DEFAULT '',
    display_name_ai TEXT NOT NULL DEFAULT '',
    description_user TEXT NOT NULL DEFAULT '',
    description_ai TEXT NOT NULL DEFAULT '',
    raw_import_data TEXT NOT NULL DEFAULT '',
    geometry_type TEXT NOT NULL DEFAULT '',
    feature_count INTEGER NOT NULL DEFAULT 0,
    crs TEXT NOT NULL DEFAULT '',
    bbox_wgs84 TEXT NOT NULL DEFAULT '[]',
    column_profile_json TEXT NOT NULL DEFAULT '{}',
    fingerprint TEXT NOT NULL DEFAULT '',
    group_id TEXT NOT NULL DEFAULT 'ungrouped',
    sort_order INTEGER NOT NULL DEFAULT 0,
    visibility INTEGER NOT NULL DEFAULT 1,
    include_in_export INTEGER NOT NULL DEFAULT 0,
    source_style_summary TEXT NOT NULL DEFAULT '',
    source_style_items_json TEXT NOT NULL DEFAULT '[]',
    cache_path TEXT NOT NULL DEFAULT '',
    ai_confidence REAL NOT NULL DEFAULT 0,
    suggested_group TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS understandings (
    dataset_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS sources (
    candidate_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    url TEXT NOT NULL,
    title TEXT NOT NULL,
    snippet TEXT NOT NULL DEFAULT '',
    domain TEXT NOT NULL DEFAULT '',
    source_type TEXT NOT NULL DEFAULT '',
    match_reason TEXT NOT NULL DEFAULT '',
    confidence REAL NOT NULL DEFAULT 0,
    is_selected INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS styles (
    dataset_id TEXT PRIMARY KEY,
    payload_json TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
);
"""

DATASET_COLUMN_MIGRATIONS = {
    "source_mtime_ns": "INTEGER NOT NULL DEFAULT 0",
    "source_size_bytes": "INTEGER NOT NULL DEFAULT 0",
    "source_style_summary": "TEXT NOT NULL DEFAULT ''",
    "source_style_items_json": "TEXT NOT NULL DEFAULT '[]'",
    "raw_import_data": "TEXT NOT NULL DEFAULT ''",
}


class CatalogRepository:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        self.ensure_group("ungrouped", "Ungrouped")

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _init_db(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA_SQL)
            self._apply_migrations(conn)
            conn.commit()

    def _apply_migrations(self, conn: sqlite3.Connection) -> None:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(datasets)").fetchall()}
        for column_name, column_spec in DATASET_COLUMN_MIGRATIONS.items():
            if column_name in columns:
                continue
            conn.execute(f"ALTER TABLE datasets ADD COLUMN {column_name} {column_spec}")

    def _group_equivalence_keys(self, group_id: str) -> set[str]:
        normalized = sanitize_group_id(group_id)
        keys = {normalized}
        if normalized == "ungrouped":
            return keys
        if "-" in normalized:
            prefix, token = normalized.rsplit("-", 1)
            prefix = f"{prefix}-"
        else:
            prefix = ""
            token = normalized
        if token.endswith("ies") and len(token) > 4 and token not in {"series", "species"}:
            keys.add(f"{prefix}{token[:-3]}y")
        if token.endswith(("sses", "shes", "ches", "xes", "zes")) and len(token) > 4:
            keys.add(f"{prefix}{token[:-2]}")
        if token.endswith("s") and len(token) > 3 and not token.endswith(("ss", "is", "us")):
            keys.add(f"{prefix}{token[:-1]}")
        return {key for key in keys if key}

    def _find_equivalent_group_id(self, conn: sqlite3.Connection, group_id: str) -> str | None:
        normalized = sanitize_group_id(group_id)
        rows = conn.execute("SELECT id FROM groups ORDER BY sort_order, name").fetchall()
        for row in rows:
            existing_group_id = str(row["id"])
            if existing_group_id == normalized:
                return existing_group_id
        candidate_keys = self._group_equivalence_keys(normalized)
        for row in rows:
            existing_group_id = str(row["id"])
            if candidate_keys.intersection(self._group_equivalence_keys(existing_group_id)):
                return existing_group_id
        return None

    def _ensure_group_with_connection(self, conn: sqlite3.Connection, group_id: str, name: str | None = None) -> str:
        normalized = sanitize_group_id(group_id)
        existing_group_id = self._find_equivalent_group_id(conn, normalized)
        if existing_group_id:
            return existing_group_id
        sort_order = conn.execute("SELECT COALESCE(MAX(sort_order), -1) + 1 FROM groups").fetchone()[0]
        conn.execute(
            "INSERT INTO groups (id, name, sort_order, created_at) VALUES (?, ?, ?, ?)",
            (normalized, name or display_group_name(normalized), sort_order, _utc_now()),
        )
        return normalized

    def ensure_group(self, group_id: str, name: str | None = None) -> str:
        with closing(self._connect()) as conn:
            ensured_group_id = self._ensure_group_with_connection(conn, group_id, name)
            conn.commit()
        return ensured_group_id

    def create_group(self, name: str) -> str:
        with closing(self._connect()) as conn:
            group_id = self._ensure_group_with_connection(conn, name, name)
            conn.commit()
        return group_id

    def list_groups(self) -> list[tuple[str, str]]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT id, name FROM groups ORDER BY sort_order, name").fetchall()
        return [(str(row["id"]), str(row["name"])) for row in rows]

    def rename_group(self, group_id: str, name: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("UPDATE groups SET name = ? WHERE id = ?", (name, group_id))
            conn.commit()

    def replace_datasets(self, datasets: list[DatasetRecord]) -> dict[str, list[str]]:
        existing = {record.dataset_id: record for record in self.list_datasets()}
        with closing(self._connect()) as conn:
            incoming_ids = {dataset.dataset_id for dataset in datasets}
            removed_ids = set(existing) - incoming_ids
            if removed_ids:
                placeholders = ", ".join("?" for _ in removed_ids)
                conn.execute(f"DELETE FROM datasets WHERE dataset_id IN ({placeholders})", tuple(sorted(removed_ids)))
            seen_groups = {"ungrouped"}
            changed_ids: list[str] = []
            reused_ids: list[str] = []
            for index, dataset in enumerate(datasets):
                prior = existing.get(dataset.dataset_id)
                if prior is not None:
                    dataset.display_name_user = prior.display_name_user
                    dataset.description_user = prior.description_user
                    dataset.visibility = prior.visibility
                    dataset.include_in_export = prior.include_in_export
                    dataset.group_id = prior.group_id
                    dataset.sort_order = prior.sort_order
                    if not dataset.cache_path:
                        dataset.cache_path = prior.cache_path
                    if self._dataset_content_changed(prior, dataset):
                        changed_ids.append(dataset.dataset_id)
                        self._delete_related_records(conn, dataset.dataset_id)
                    else:
                        if not dataset.display_name_ai:
                            dataset.display_name_ai = prior.display_name_ai
                        if not dataset.description_ai:
                            dataset.description_ai = prior.description_ai
                        if not dataset.suggested_group:
                            dataset.suggested_group = prior.suggested_group
                        if not dataset.ai_confidence:
                            dataset.ai_confidence = prior.ai_confidence
                        reused_ids.append(dataset.dataset_id)
                else:
                    changed_ids.append(dataset.dataset_id)
                if dataset.group_id:
                    seen_groups.add(dataset.group_id)
                if dataset.suggested_group:
                    seen_groups.add(dataset.suggested_group)
                conn.execute(
                    """
                    INSERT INTO datasets (
                        dataset_id, source_path, source_format, source_mtime_ns, source_size_bytes, layer_name,
                        display_name_user, display_name_ai, description_user, description_ai, raw_import_data,
                        geometry_type, feature_count, crs, bbox_wgs84, column_profile_json,
                        fingerprint, group_id, sort_order, visibility, include_in_export, source_style_summary, source_style_items_json,
                        cache_path, ai_confidence, suggested_group, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(dataset_id) DO UPDATE SET
                        source_path = excluded.source_path,
                        source_format = excluded.source_format,
                        source_mtime_ns = excluded.source_mtime_ns,
                        source_size_bytes = excluded.source_size_bytes,
                        layer_name = excluded.layer_name,
                        display_name_user = excluded.display_name_user,
                        display_name_ai = excluded.display_name_ai,
                        description_user = excluded.description_user,
                        description_ai = excluded.description_ai,
                        raw_import_data = excluded.raw_import_data,
                        geometry_type = excluded.geometry_type,
                        feature_count = excluded.feature_count,
                        crs = excluded.crs,
                        bbox_wgs84 = excluded.bbox_wgs84,
                        column_profile_json = excluded.column_profile_json,
                        fingerprint = excluded.fingerprint,
                        group_id = excluded.group_id,
                        sort_order = excluded.sort_order,
                        visibility = excluded.visibility,
                        include_in_export = excluded.include_in_export,
                        source_style_summary = excluded.source_style_summary,
                        source_style_items_json = excluded.source_style_items_json,
                        cache_path = excluded.cache_path,
                        ai_confidence = excluded.ai_confidence,
                        suggested_group = excluded.suggested_group,
                        updated_at = excluded.updated_at
                    """,
                    (
                        dataset.dataset_id,
                        dataset.source_path,
                        dataset.source_format,
                        int(dataset.source_mtime_ns or 0),
                        int(dataset.source_size_bytes or 0),
                        dataset.layer_name,
                        dataset.display_name_user,
                        dataset.display_name_ai,
                        dataset.description_user,
                        dataset.description_ai,
                        dataset.raw_import_data,
                        dataset.geometry_type,
                        dataset.feature_count,
                        dataset.crs,
                        json.dumps(dataset.bbox_wgs84),
                        dataset.column_profile_json,
                        dataset.fingerprint,
                        dataset.group_id or "ungrouped",
                        dataset.sort_order if dataset.sort_order else index,
                        int(dataset.visibility),
                        int(dataset.include_in_export),
                        dataset.source_style_summary,
                        dataset.source_style_items_json,
                        dataset.cache_path,
                        dataset.ai_confidence,
                        dataset.suggested_group,
                        _utc_now(),
                    ),
                )
            conn.commit()
        for group_id in seen_groups:
            self.ensure_group(group_id)
        return {
            "changed_ids": changed_ids,
            "reused_ids": reused_ids,
            "removed_ids": sorted(removed_ids),
        }

    def list_datasets(self) -> list[DatasetRecord]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT dataset_id, source_path, source_format, layer_name,
                       source_mtime_ns, source_size_bytes,
                       display_name_user, display_name_ai, description_user, description_ai, raw_import_data,
                       geometry_type, feature_count, crs, bbox_wgs84, column_profile_json,
                       fingerprint, group_id, sort_order, visibility, include_in_export, source_style_summary, source_style_items_json,
                       cache_path, ai_confidence, suggested_group
                FROM datasets
                ORDER BY group_id, sort_order, COALESCE(display_name_user, display_name_ai, layer_name, source_path)
                """
            ).fetchall()
        return [DatasetRecord.from_row(dict(row)) for row in rows]

    def get_dataset(self, dataset_id: str) -> DatasetRecord | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                """
                SELECT dataset_id, source_path, source_format, layer_name,
                       source_mtime_ns, source_size_bytes,
                       display_name_user, display_name_ai, description_user, description_ai, raw_import_data,
                       geometry_type, feature_count, crs, bbox_wgs84, column_profile_json,
                       fingerprint, group_id, sort_order, visibility, include_in_export, source_style_summary, source_style_items_json,
                       cache_path, ai_confidence, suggested_group
                FROM datasets
                WHERE dataset_id = ?
                """,
                (dataset_id,),
            ).fetchone()
        if not row:
            return None
        return DatasetRecord.from_row(dict(row))

    def save_dataset_user_fields(
        self,
        dataset_id: str,
        *,
        display_name_user: str,
        description_user: str,
        visibility: bool,
        include_in_export: bool,
    ) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                UPDATE datasets
                SET display_name_user = ?, description_user = ?, visibility = ?, include_in_export = ?, updated_at = ?
                WHERE dataset_id = ?
                """,
                (
                    display_name_user,
                    description_user,
                    int(visibility),
                    int(include_in_export),
                    _utc_now(),
                    dataset_id,
                ),
            )
            conn.commit()

    def fill_empty_user_fields_from_ai(self, dataset_ids: list[str]) -> int:
        return self._apply_ai_user_field_transfer(dataset_ids, overwrite_existing=False)

    def transfer_user_fields_from_ai(self, dataset_ids: list[str]) -> int:
        return self._apply_ai_user_field_transfer(dataset_ids, overwrite_existing=True)

    def _apply_ai_user_field_transfer(self, dataset_ids: list[str], *, overwrite_existing: bool) -> int:
        if not dataset_ids:
            return 0
        placeholders = ", ".join("?" for _ in dataset_ids)
        updated_rows = 0
        with closing(self._connect()) as conn:
            rows = conn.execute(
                f"""
                SELECT dataset_id, display_name_user, description_user, display_name_ai, description_ai,
                       visibility, include_in_export
                FROM datasets
                WHERE dataset_id IN ({placeholders})
                """,
                dataset_ids,
            ).fetchall()
            updates: list[tuple[str, str, str]] = []
            for row in rows:
                display_name_user = str(row["display_name_user"] or "")
                description_user = str(row["description_user"] or "")
                display_name_ai = str(row["display_name_ai"] or "")
                description_ai = str(row["description_ai"] or "")
                if overwrite_existing:
                    new_name = display_name_ai or display_name_user
                    new_description = description_ai or description_user
                else:
                    new_name = display_name_user or display_name_ai
                    new_description = description_user or description_ai
                if new_name == display_name_user and new_description == description_user:
                    continue
                updates.append((new_name, new_description, str(row["dataset_id"])))
            if updates:
                conn.executemany(
                    """
                    UPDATE datasets
                    SET display_name_user = ?, description_user = ?, updated_at = ?
                    WHERE dataset_id = ?
                    """,
                    [
                        (display_name_user, description_user, _utc_now(), dataset_id)
                        for display_name_user, description_user, dataset_id in updates
                    ],
                )
                updated_rows = len(updates)
                conn.commit()
        return updated_rows

    def set_visibility_for_datasets(self, dataset_ids: list[str], visibility: bool) -> None:
        if not dataset_ids:
            return
        placeholders = ", ".join("?" for _ in dataset_ids)
        with closing(self._connect()) as conn:
            conn.execute(
                f"UPDATE datasets SET visibility = ?, updated_at = ? WHERE dataset_id IN ({placeholders})",
                (int(visibility), _utc_now(), *dataset_ids),
            )
            conn.commit()

    def set_visibility_for_group(self, group_id: str, visibility: bool) -> int:
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                "UPDATE datasets SET visibility = ?, updated_at = ? WHERE group_id = ?",
                (int(visibility), _utc_now(), group_id),
            )
            conn.commit()
        return int(cursor.rowcount or 0)

    def set_include_in_export_for_datasets(self, dataset_ids: list[str], include_in_export: bool) -> None:
        if not dataset_ids:
            return
        placeholders = ", ".join("?" for _ in dataset_ids)
        with closing(self._connect()) as conn:
            conn.execute(
                f"UPDATE datasets SET include_in_export = ?, updated_at = ? WHERE dataset_id IN ({placeholders})",
                (int(include_in_export), _utc_now(), *dataset_ids),
            )
            conn.commit()

    def update_ordering(self, group_order: list[str], dataset_order: list[tuple[str, str, int]]) -> None:
        with closing(self._connect()) as conn:
            for index, group_id in enumerate(group_order):
                conn.execute("UPDATE groups SET sort_order = ? WHERE id = ?", (index, group_id))
            for dataset_id, group_id, sort_order in dataset_order:
                conn.execute(
                    "UPDATE datasets SET group_id = ?, sort_order = ?, updated_at = ? WHERE dataset_id = ?",
                    (group_id, sort_order, _utc_now(), dataset_id),
                )
            conn.commit()

    def upsert_understanding(self, dataset_id: str, understanding: DatasetUnderstanding) -> None:
        self.upsert_understandings_bulk([(dataset_id, understanding)])

    def upsert_understandings_bulk(
        self,
        updates: list[tuple[str, DatasetUnderstanding]],
        *,
        auto_assign_group: bool = False,
    ) -> int:
        if not updates:
            return 0
        normalized_updates: list[tuple[str, str, str, str, float, str]] = []
        suggested_groups: set[str] = set()
        dataset_ids: list[str] = []
        for dataset_id, understanding in updates:
            suggested_group = sanitize_group_id(understanding.suggested_group or "ungrouped")
            normalized_updates.append(
                (
                    dataset_id,
                    understanding.to_json(),
                    understanding.suggested_title,
                    understanding.suggested_description,
                    float(understanding.confidence or 0.0),
                    suggested_group,
                )
            )
            suggested_groups.add(suggested_group)
            dataset_ids.append(dataset_id)
        with closing(self._connect()) as conn:
            current_groups: dict[str, str] = {}
            if auto_assign_group:
                placeholders = ", ".join("?" for _ in dataset_ids)
                rows = conn.execute(
                    f"SELECT dataset_id, group_id FROM datasets WHERE dataset_id IN ({placeholders})",
                    dataset_ids,
                ).fetchall()
                current_groups = {
                    str(row["dataset_id"]): sanitize_group_id(str(row["group_id"] or "ungrouped"))
                    for row in rows
                }
            resolved_suggested_groups = {
                group_id: (
                    self._ensure_group_with_connection(conn, group_id)
                    if auto_assign_group
                    else self._find_equivalent_group_id(conn, group_id) or group_id
                )
                for group_id in sorted(suggested_groups)
            }
            for dataset_id, payload, suggested_title, suggested_description, confidence, suggested_group in normalized_updates:
                resolved_group_id = resolved_suggested_groups.get(suggested_group, suggested_group)
                updated_at = _utc_now()
                conn.execute(
                    """
                    INSERT INTO understandings (dataset_id, payload_json, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(dataset_id) DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at
                    """,
                    (dataset_id, payload, updated_at),
                )
                conn.execute(
                    """
                    UPDATE datasets
                    SET display_name_ai = ?, description_ai = ?, ai_confidence = ?, suggested_group = ?, updated_at = ?
                    WHERE dataset_id = ?
                    """,
                    (
                        suggested_title,
                        suggested_description,
                        confidence,
                        resolved_group_id,
                        updated_at,
                        dataset_id,
                    ),
                )
                if auto_assign_group and current_groups.get(dataset_id, "ungrouped") in {"", "ungrouped"} and resolved_group_id:
                    conn.execute(
                        "UPDATE datasets SET group_id = ?, updated_at = ? WHERE dataset_id = ?",
                        (resolved_group_id, updated_at, dataset_id),
                    )
            conn.commit()
        return len(normalized_updates)

    def get_understanding(self, dataset_id: str) -> DatasetUnderstanding:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT payload_json FROM understandings WHERE dataset_id = ?", (dataset_id,)).fetchone()
        if not row:
            return DatasetUnderstanding()
        return DatasetUnderstanding.from_json(str(row["payload_json"]))

    def replace_sources(self, dataset_id: str, sources: list[SourceCandidate]) -> None:
        normalized_sources: list[SourceCandidate] = [
            SourceCandidate(
                url=source.url,
                title=source.title,
                snippet=source.snippet,
                domain=source.domain,
                source_type=source.source_type,
                match_reason=source.match_reason,
                confidence=source.confidence,
                is_selected=False,
                candidate_id=source.candidate_id,
            )
            for source in sources
        ]
        if normalized_sources:
            highest_confidence = max(float(source.confidence) for source in normalized_sources)
            top_indexes = [
                index
                for index, source in enumerate(normalized_sources)
                if float(source.confidence) == highest_confidence
            ]
            selected_index = random.choice(top_indexes)
            normalized_sources[selected_index].is_selected = True
        with closing(self._connect()) as conn:
            conn.execute("DELETE FROM sources WHERE dataset_id = ?", (dataset_id,))
            for source in normalized_sources:
                candidate_id = source.candidate_id or uuid.uuid4().hex
                conn.execute(
                    """
                    INSERT INTO sources (
                        candidate_id, dataset_id, url, title, snippet, domain,
                        source_type, match_reason, confidence, is_selected
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        dataset_id,
                        source.url,
                        source.title,
                        source.snippet,
                        source.domain,
                        source.source_type,
                        source.match_reason,
                        source.confidence,
                        int(source.is_selected),
                    ),
                )
            conn.commit()

    def list_sources(self, dataset_id: str) -> list[SourceCandidate]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT url, title, snippet, domain, source_type, match_reason, confidence, is_selected, candidate_id
                FROM sources
                WHERE dataset_id = ?
                ORDER BY is_selected DESC, confidence DESC, title
                """,
                (dataset_id,),
            ).fetchall()
        return [SourceCandidate.from_row(dict(row)) for row in rows]

    def upsert_style(self, dataset_id: str, style: LayerStyle) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO styles (dataset_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(dataset_id) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (dataset_id, style.to_json(), _utc_now()),
            )
            conn.commit()

    def get_style(self, dataset_id: str) -> LayerStyle | None:
        with closing(self._connect()) as conn:
            row = conn.execute("SELECT payload_json FROM styles WHERE dataset_id = ?", (dataset_id,)).fetchone()
        if not row:
            return None
        return LayerStyle.from_json(str(row["payload_json"]))

    def list_styles(self) -> dict[str, LayerStyle]:
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT dataset_id, payload_json FROM styles").fetchall()
        styles: dict[str, LayerStyle] = {}
        for row in rows:
            style = LayerStyle.from_json(str(row["payload_json"]))
            if style is not None:
                styles[str(row["dataset_id"])] = style
        return styles

    def select_source(self, dataset_id: str, candidate_id: str) -> None:
        with closing(self._connect()) as conn:
            conn.execute("UPDATE sources SET is_selected = 0 WHERE dataset_id = ?", (dataset_id,))
            conn.execute(
                "UPDATE sources SET is_selected = 1 WHERE dataset_id = ? AND candidate_id = ?",
                (dataset_id, candidate_id),
            )
            conn.commit()

    def apply_suggested_group(self, dataset_id: str) -> None:
        dataset = self.get_dataset(dataset_id)
        if not dataset or not dataset.suggested_group:
            return
        resolved_group_id = self.ensure_group(dataset.suggested_group)
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE datasets SET group_id = ?, updated_at = ? WHERE dataset_id = ?",
                (resolved_group_id, _utc_now(), dataset_id),
            )
            conn.commit()

    def assign_group(self, dataset_id: str, group_id: str) -> None:
        normalized = self.ensure_group(group_id or "ungrouped")
        with closing(self._connect()) as conn:
            conn.execute(
                "UPDATE datasets SET group_id = ?, updated_at = ? WHERE dataset_id = ?",
                (normalized, _utc_now(), dataset_id),
            )
            conn.commit()

    def reset_groups(self, dataset_ids: list[str]) -> int:
        if not dataset_ids:
            return 0
        placeholders = ", ".join("?" for _ in dataset_ids)
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                f"UPDATE datasets SET group_id = 'ungrouped', updated_at = ? WHERE dataset_id IN ({placeholders})",
                (_utc_now(), *dataset_ids),
            )
            conn.commit()
        self.prune_empty_groups()
        return int(cursor.rowcount or 0)

    def assign_groups_bulk(self, assignments: dict[str, str]) -> int:
        normalized_assignments: list[tuple[str, str, str]] = []
        for dataset_id, group_name in assignments.items():
            display_name = str(group_name or "").strip() or "Ungrouped"
            normalized_group_id = self.ensure_group(display_name, display_name)
            normalized_assignments.append((dataset_id, normalized_group_id, display_name))
        if not normalized_assignments:
            return 0
        with closing(self._connect()) as conn:
            for dataset_id, normalized_group_id, _display_name in normalized_assignments:
                conn.execute(
                    "UPDATE datasets SET group_id = ?, updated_at = ? WHERE dataset_id = ?",
                    (normalized_group_id, _utc_now(), dataset_id),
                )
            conn.commit()
        self.prune_empty_groups()
        return len(normalized_assignments)

    def prune_empty_groups(self) -> int:
        with closing(self._connect()) as conn:
            cursor = conn.execute(
                """
                DELETE FROM groups
                WHERE id != 'ungrouped'
                  AND id NOT IN (SELECT DISTINCT group_id FROM datasets)
                """
            )
            conn.commit()
        return int(cursor.rowcount or 0)

    def summary(self) -> dict[str, int]:
        with closing(self._connect()) as conn:
            dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
            group_count = conn.execute("SELECT COUNT(*) FROM groups").fetchone()[0]
            source_count = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
            style_count = conn.execute("SELECT COUNT(*) FROM styles").fetchone()[0]
        return {
            "dataset_count": int(dataset_count),
            "group_count": int(group_count),
            "source_count": int(source_count),
            "style_count": int(style_count),
        }

    def _delete_related_records(self, conn: sqlite3.Connection, dataset_id: str) -> None:
        conn.execute("DELETE FROM understandings WHERE dataset_id = ?", (dataset_id,))
        conn.execute("DELETE FROM sources WHERE dataset_id = ?", (dataset_id,))

    def _dataset_content_changed(self, prior: DatasetRecord, current: DatasetRecord) -> bool:
        return any(
            [
                str(prior.source_path) != str(current.source_path),
                str(prior.layer_name) != str(current.layer_name),
                int(prior.source_mtime_ns or 0) != int(current.source_mtime_ns or 0),
                int(prior.source_size_bytes or 0) != int(current.source_size_bytes or 0),
                str(prior.fingerprint or "") != str(current.fingerprint or ""),
                str(prior.raw_import_data or "") != str(current.raw_import_data or ""),
                str(prior.geometry_type or "") != str(current.geometry_type or ""),
                int(prior.feature_count or 0) != int(current.feature_count or 0),
            ]
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

