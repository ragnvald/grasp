from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from typing import Any


@dataclass(slots=True)
class DatasetRecord:
    dataset_id: str
    source_path: str
    source_format: str
    source_mtime_ns: int = 0
    source_size_bytes: int = 0
    layer_name: str = ""
    display_name_user: str = ""
    display_name_ai: str = ""
    description_user: str = ""
    description_ai: str = ""
    geometry_type: str = ""
    feature_count: int = 0
    crs: str = ""
    bbox_wgs84: list[float] = field(default_factory=list)
    column_profile_json: str = "{}"
    fingerprint: str = ""
    group_id: str = "ungrouped"
    sort_order: int = 0
    visibility: bool = True
    include_in_export: bool = False
    source_style_summary: str = ""
    source_style_items_json: str = "[]"
    cache_path: str = ""
    ai_confidence: float = 0.0
    suggested_group: str = ""

    @property
    def preferred_name(self) -> str:
        return self.display_name_user.strip() or self.display_name_ai.strip() or self.default_name

    @property
    def preferred_description(self) -> str:
        return self.description_user.strip() or self.description_ai.strip()

    @property
    def default_name(self) -> str:
        if self.layer_name:
            return self.layer_name
        return self.source_basename

    @property
    def source_basename(self) -> str:
        normalized = self.source_path.replace("\\", "/").rstrip("/")
        return normalized.split("/")[-1]

    @property
    def column_profile(self) -> dict[str, Any]:
        try:
            return json.loads(self.column_profile_json or "{}")
        except json.JSONDecodeError:
            return {}

    @property
    def source_style_items(self) -> list[dict[str, Any]]:
        try:
            payload = json.loads(self.source_style_items_json or "[]")
        except json.JSONDecodeError:
            return []
        if not isinstance(payload, list):
            return []
        return [item for item in payload if isinstance(item, dict)]

    @property
    def has_source_style(self) -> bool:
        return bool(self.source_style_summary.strip() or self.source_style_items)

    def to_row(self) -> dict[str, Any]:
        row = asdict(self)
        row["visibility"] = int(self.visibility)
        row["include_in_export"] = int(self.include_in_export)
        return row

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "DatasetRecord":
        data = dict(row)
        data["visibility"] = bool(data.get("visibility", 1))
        data["include_in_export"] = bool(data.get("include_in_export", 0))
        data["bbox_wgs84"] = json.loads(data.get("bbox_wgs84") or "[]")
        return cls(**data)


@dataclass(slots=True)
class DatasetUnderstanding:
    theme: str = ""
    keywords: list[str] = field(default_factory=list)
    place_names: list[str] = field(default_factory=list)
    suggested_title: str = ""
    suggested_description: str = ""
    suggested_group: str = ""
    search_queries: list[str] = field(default_factory=list)
    confidence: float = 0.0

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, payload: str | None) -> "DatasetUnderstanding":
        if not payload:
            return cls()
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return cls()
        return cls(**data)


@dataclass(slots=True)
class SourceCandidate:
    url: str
    title: str
    snippet: str = ""
    domain: str = ""
    source_type: str = ""
    match_reason: str = ""
    confidence: float = 0.0
    is_selected: bool = False
    candidate_id: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "SourceCandidate":
        data = dict(row)
        data["is_selected"] = bool(data.get("is_selected", 0))
        return cls(**data)


@dataclass(slots=True)
class LayerStyle:
    label: str = ""
    summary: str = ""
    theme: str = ""
    stroke_color: str = "#6d597a"
    fill_color: str = "#b08968"
    fill_opacity: float = 0.24
    stroke_width: float = 1.5
    line_opacity: float = 0.95
    point_radius: float = 6.0
    point_stroke_color: str = "#fffdf8"
    point_stroke_width: float = 1.2
    point_fill_opacity: float = 0.9

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    def to_map_payload(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_json(cls, payload: str | None) -> "LayerStyle" | None:
        if not payload:
            return None
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        try:
            return cls(**data)
        except TypeError:
            return None
