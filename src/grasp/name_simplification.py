from __future__ import annotations

from dataclasses import dataclass
import re


MAX_SIMPLIFIED_DATASET_NAME_LENGTH = 100
_HIERARCHY_SPLIT_PATTERN = re.compile(r"__+")
_TOKEN_SPLIT_PATTERN = re.compile(r"[_\-.]+")
_SPACE_PATTERN = re.compile(r"\s+")
_KNOWN_VECTOR_SUFFIXES = {".geojson", ".json", ".shp", ".gpkg", ".parquet", ".fgb", ".kml", ".gml"}
_GENERIC_SHORT_NAME_TOKENS = {"data", "dataset", "layer", "table"}


@dataclass(slots=True)
class SimplifiedDatasetName:
    display_name: str
    description_note: str


def suggest_simplified_dataset_name(raw_name: str, *, source_kind: str = "layer") -> SimplifiedDatasetName | None:
    original_name = _normalize_spaces(str(raw_name or "").strip())
    if not original_name:
        return None
    working_name = _strip_known_vector_suffix(original_name)
    hierarchical_segments = [segment for segment in _HIERARCHY_SPLIT_PATTERN.split(working_name) if segment.strip()]
    if len(hierarchical_segments) > 1:
        human_segments = _deduplicate_adjacent([_humanize_segment(segment) for segment in hierarchical_segments])
        if not human_segments:
            return None
        display_name = human_segments[-1]
        context_segments = human_segments[:-1]
    else:
        if not _looks_like_technical_name(working_name):
            return None
        words = _humanize_segment(working_name).split()
        if len(words) <= 4:
            return None
        display_name = " ".join(words[-4:])
        context_segments = []
    display_name = _humanize_segment(display_name)
    if _comparison_token(display_name) in _GENERIC_SHORT_NAME_TOKENS and context_segments:
        display_name = f"{context_segments[-1]} {display_name}"
    display_name = _truncate_display_name(display_name.strip())
    if not display_name:
        return None
    original_display = _humanize_segment(working_name)
    if _comparison_token(display_name) == _comparison_token(original_display):
        return None
    description_parts = ["Imported name simplified from source naming."]
    if context_segments:
        description_parts.append(f"Source naming context: {' > '.join(context_segments)}.")
    description_parts.append(f"Original source {source_kind} name: {original_name}.")
    return SimplifiedDatasetName(display_name=display_name, description_note=" ".join(description_parts))


def _looks_like_technical_name(value: str) -> bool:
    separator_count = sum(value.count(separator) for separator in "_-.")
    return separator_count >= 5 or (len(value) >= 48 and separator_count >= 3)


def _humanize_segment(value: str) -> str:
    humanized = _TOKEN_SPLIT_PATTERN.sub(" ", str(value or "").strip(" _-."))
    humanized = _normalize_spaces(humanized)
    if not humanized:
        return ""
    return humanized[:1].upper() + humanized[1:]


def _strip_known_vector_suffix(value: str) -> str:
    lowered = value.lower()
    for suffix in _KNOWN_VECTOR_SUFFIXES:
        if lowered.endswith(suffix):
            return value[: -len(suffix)]
    return value


def _deduplicate_adjacent(segments: list[str]) -> list[str]:
    unique: list[str] = []
    previous_token = ""
    for segment in segments:
        token = _comparison_token(segment)
        if not token:
            continue
        if token == previous_token:
            continue
        unique.append(segment)
        previous_token = token
    return unique


def _truncate_display_name(value: str) -> str:
    if len(value) <= MAX_SIMPLIFIED_DATASET_NAME_LENGTH:
        return value
    cutoff = MAX_SIMPLIFIED_DATASET_NAME_LENGTH - len("(..)")
    return value[:cutoff].rstrip() + "(..)"


def _comparison_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _normalize_spaces(value: str) -> str:
    return _SPACE_PATTERN.sub(" ", str(value or "")).strip()
