from __future__ import annotations

from abc import ABC, abstractmethod
import json
import os
from pathlib import Path
import re
from typing import Iterable
from urllib.parse import quote, urlparse

import requests

from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate

DEFAULT_OPENAI_MODEL = "gpt-4o-mini"
DEFAULT_OPENAI_ENDPOINT = "https://api.openai.com/v1/chat/completions"
DEFAULT_OPENAI_TIMEOUT_S = 20.0
DEFAULT_OPENAI_MAX_CONSECUTIVE_FAILURES = 2
DEFAULT_SEARCH_TIMEOUT_S = 4.0
DEFAULT_SEARCH_MAX_CONSECUTIVE_FAILURES = 1
DEFAULT_SEARCH_TARGET_CANDIDATES = 5
DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME = True
DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME = True
DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES = True
DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES = False
DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE = False
DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT = False
DEFAULT_CLASSIFICATION_INCLUDE_BBOX = False
GENERIC_THEME = "general geographic"
GENERIC_DESCRIPTION_MARKERS = (
    "general geographic",
    "general geospatial",
    "general spatial",
    "generic geographic",
    "geographic data",
)


class ClassificationProvider(ABC):
    @abstractmethod
    def classify(self, dataset: DatasetRecord) -> DatasetUnderstanding:
        raise NotImplementedError


class SourceSearchProvider(ABC):
    @abstractmethod
    def search(self, queries: Iterable[str]) -> list[SourceCandidate]:
        raise NotImplementedError


class CandidateRanker(ABC):
    @abstractmethod
    def rank(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> list[SourceCandidate]:
        raise NotImplementedError


class HeuristicClassificationProvider(ClassificationProvider):
    THEMES = {
        "transport": {"road", "rail", "street", "transport", "route", "line"},
        "hydrology": {"water", "river", "lake", "stream", "coast", "coastal", "costeiro", "costeiros", "ocean"},
        "risk": {
            "risk",
            "risco",
            "hazard",
            "fire",
            "wildfire",
            "incendio",
            "incêndio",
            "incandio",
            "queimada",
            "queimadas",
            "flood",
            "flooding",
            "inundacao",
            "inundação",
            "cyclone",
            "ciclone",
            "drought",
            "seca",
            "erosion",
            "erosao",
            "erosão",
            "seismic",
            "sismica",
            "sísmica",
        },
        "administrative": {
            "admin",
            "administrative",
            "administrativo",
            "kommune",
            "county",
            "municipality",
            "district",
            "distrito",
            "distritos",
            "boundary",
            "boundaries",
            "province",
            "region",
        },
        "habitat": {"nature", "habitat", "species", "forest", "wetland", "ecology"},
        "land-use": {"landuse", "parcel", "zoning", "building", "property"},
        "protected-area": {
            "protected",
            "area",
            "areas",
            "park",
            "parque",
            "national",
            "nacional",
            "reserve",
            "reserva",
            "conservation",
            "conservacao",
            "conservancy",
            "patrimonio",
            "heritage",
        },
    }
    DESCRIPTION_STOPWORDS = {
        "data",
        "dataset",
        "layer",
        "table",
        "map",
        "geojson",
        "parquet",
        "shape",
        "shp",
        "gpkg",
        "file",
        "simplified",
        "simplify",
    }
    PLACE_EXCLUSION_TOKENS = {
        "admin",
        "administrative",
        "administrativo",
        "district",
        "distrito",
        "distritos",
        "boundary",
        "boundaries",
        "coast",
        "coastal",
        "costeiro",
        "costeiros",
        "park",
        "parque",
        "national",
        "nacional",
        "protected",
        "reserve",
        "reserva",
        "conservation",
        "conservacao",
        "heritage",
        "patrimonio",
        "roads",
        "road",
        "risk",
        "risco",
        "hazard",
        "fire",
        "wildfire",
        "incendio",
        "incêndio",
        "incandio",
        "queimada",
        "queimadas",
        "flood",
        "flooding",
        "inundacao",
        "inundação",
        "cyclone",
        "ciclone",
        "drought",
        "seca",
        "erosion",
        "erosao",
        "erosão",
        "seismic",
        "sismica",
        "sísmica",
        "water",
        "river",
        "lake",
        "line",
        "polygon",
        "point",
    }

    def classify(self, dataset: DatasetRecord) -> DatasetUnderstanding:
        profile = dataset.column_profile
        title = _title_case(dataset.layer_name or Path(dataset.source_path).stem)
        source_tokens = _tokenize(
            " ".join(
                [
                    title,
                    Path(dataset.source_path).stem,
                    dataset.layer_name,
                    dataset.geometry_type,
                    " ".join(column["name"] for column in profile.get("columns", [])),
                ]
            )
        )
        theme = self._theme_for_tokens(source_tokens)
        keywords = self._keywords_for_tokens(source_tokens)
        place_names = self._dedupe(self._extract_places(profile) + self._extract_place_hints_from_name(title), limit=5)
        description = self._build_description(
            dataset=dataset,
            title=title,
            theme=theme,
            keywords=keywords,
            place_names=place_names,
        )
        group = self._group_for_dataset(title, theme, keywords)
        queries = self._search_queries(title, theme, place_names, keywords)
        confidence = 0.45 + min(0.4, len(keywords) * 0.03)
        return DatasetUnderstanding(
            theme=theme,
            keywords=keywords,
            place_names=place_names,
            suggested_title=title,
            suggested_description=description,
            suggested_group=group,
            search_queries=queries,
            confidence=round(min(confidence, 0.92), 2),
        )

    def enrich_from_sources(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> DatasetUnderstanding:
        title = understanding.suggested_title or _title_case(dataset.layer_name or Path(dataset.source_path).stem)
        candidate_text = " ".join(
            part
            for candidate in candidates[:3]
            for part in (candidate.title, candidate.snippet, candidate.domain)
            if part
        )
        combined_tokens = _tokenize(
            " ".join(
                [
                    title,
                    dataset.layer_name,
                    Path(dataset.source_path).stem,
                    understanding.theme,
                    " ".join(understanding.keywords),
                    " ".join(understanding.place_names),
                    candidate_text,
                ]
            )
        )
        theme = understanding.theme or self._theme_for_tokens(combined_tokens)
        keywords = self._dedupe(understanding.keywords + self._keywords_for_tokens(combined_tokens), limit=8)
        place_names = self._dedupe(
            understanding.place_names + self._extract_places(dataset.column_profile) + self._extract_place_hints_from_name(title),
            limit=5,
        )
        description = self._build_description(
            dataset=dataset,
            title=title,
            theme=theme,
            keywords=keywords,
            place_names=place_names,
            top_candidate=self._best_descriptive_candidate(candidates),
            current_description=understanding.suggested_description,
        )
        group = understanding.suggested_group or self._group_for_dataset(title, theme, keywords)
        queries = understanding.search_queries or self._search_queries(title, theme, place_names, keywords)
        confidence = understanding.confidence
        if self._best_descriptive_candidate(candidates) is not None:
            confidence = max(confidence, 0.62)
        return DatasetUnderstanding(
            theme=theme,
            keywords=keywords,
            place_names=place_names,
            suggested_title=title,
            suggested_description=description,
            suggested_group=group,
            search_queries=queries,
            confidence=round(min(confidence or 0.0, 0.95), 2),
        )

    def group_datasets(
        self,
        datasets: list[DatasetRecord],
        target_group_count: int,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, str]:
        if not datasets:
            return {}
        target = max(1, min(int(target_group_count or 1), len(datasets)))
        profiles = [self._group_profile(dataset) for dataset in datasets]
        return self._assign_profiles_to_labels(profiles, target)

    def assignments_look_too_broad(
        self,
        datasets: list[DatasetRecord],
        assignments: dict[str, str],
        target_group_count: int,
    ) -> bool:
        if not datasets or not assignments:
            return False
        target = max(1, min(int(target_group_count or 1), len(datasets)))
        profiles = [self._group_profile(dataset) for dataset in datasets]
        profile_by_id = {profile["dataset"].dataset_id: profile for profile in profiles}
        grouped: dict[str, list[str]] = {}
        for dataset in datasets:
            group_name = str(assignments.get(dataset.dataset_id) or "").strip()
            if not group_name:
                continue
            grouped.setdefault(group_name, []).append(dataset.dataset_id)
        if not grouped:
            return False
        average_group_size = max(1, (len(datasets) + target - 1) // target)
        if len(datasets) >= max(8, target * 2) and len(grouped) < max(2, target // 2):
            return True
        for dataset_ids in grouped.values():
            group_size = len(dataset_ids)
            if group_size > max(18, average_group_size * 3):
                return True
            if group_size < max(6, average_group_size * 2):
                continue
            theme_counts: dict[str, int] = {}
            for dataset_id in dataset_ids:
                theme = str(profile_by_id.get(dataset_id, {}).get("theme") or "")
                if not theme or theme == GENERIC_THEME:
                    continue
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
            if len(theme_counts) >= 3:
                return True
            if theme_counts:
                dominant_share = max(theme_counts.values()) / max(1, group_size)
                if dominant_share < 0.7:
                    return True
        return False

    def _theme_for_tokens(self, tokens: set[str]) -> str:
        best_theme = GENERIC_THEME
        best_score = 0
        for theme, marker_tokens in self.THEMES.items():
            score = len(tokens.intersection(marker_tokens))
            if score > best_score:
                best_theme = theme
                best_score = score
        return best_theme

    def _extract_places(self, profile: dict) -> list[str]:
        places: list[str] = []
        for column in profile.get("columns", []):
            name = str(column.get("name") or "").lower()
            if not any(token in name for token in ("kommune", "county", "municip", "city", "place", "region")):
                continue
            for sample in column.get("samples", []):
                cleaned = str(sample).strip()
                if cleaned and cleaned not in places:
                    places.append(cleaned)
                if len(places) >= 3:
                    return places
        return places

    def _group_for_dataset(self, title: str, theme: str, keywords: list[str]) -> str:
        if theme != GENERIC_THEME:
            return theme.replace(" ", "-")
        title_tokens = _meaningful_name_tokens(title)
        if title_tokens:
            return "-".join(title_tokens[:2])
        keyword_tokens = [token for token in keywords if token not in {"data", "dataset", "layer"}]
        if keyword_tokens:
            return "-".join(keyword_tokens[:2])
        return "ungrouped"

    def _search_queries(self, title: str, theme: str, place_names: list[str], keywords: list[str]) -> list[str]:
        place = place_names[0] if place_names else ""
        query_one = " ".join(part for part in (f"\"{title}\"", place, "geodata") if part)
        query_two = " ".join(part for part in (self._subject_query(theme, keywords), place, "dataset") if part)
        query_three = " ".join(part for part in (title, " ".join(keywords[:3]), "download") if part)
        return [query_one.strip(), query_two.strip(), query_three.strip()]

    def _keywords_for_tokens(self, tokens: set[str]) -> list[str]:
        cleaned = [token for token in sorted(tokens) if token not in self.DESCRIPTION_STOPWORDS]
        return cleaned[:8]

    def _extract_place_hints_from_name(self, value: str) -> list[str]:
        raw_tokens = re.findall(r"[0-9A-Za-z\u00C0-\u00FF]+", value)
        hints: list[str] = []
        current: list[str] = []
        for token in raw_tokens:
            normalized = token.lower()
            if normalized in self.PLACE_EXCLUSION_TOKENS or normalized in self.DESCRIPTION_STOPWORDS:
                if current:
                    hints.append(" ".join(current))
                    current = []
                continue
            if len(normalized) <= 2:
                continue
            current.append(token)
        if current:
            hints.append(" ".join(current))
        return hints[:2]

    def _build_description(
        self,
        *,
        dataset: DatasetRecord,
        title: str,
        theme: str,
        keywords: list[str],
        place_names: list[str],
        top_candidate: SourceCandidate | None = None,
        current_description: str = "",
    ) -> str:
        support_sentence = self._support_sentence(top_candidate)
        if current_description and not _description_is_generic(current_description):
            if support_sentence and support_sentence not in current_description:
                return f"{current_description.strip()} {support_sentence}"
            return current_description.strip()

        geometry_phrase = self._geometry_phrase(dataset.geometry_type)
        lead_sentence = f"{title} is {geometry_phrase} with {dataset.feature_count} feature(s)."

        subject = self._subject_phrase(theme, keywords)
        place = self._place_phrase(place_names)
        context_sentence = ""
        if subject:
            context_sentence = f"The file and layer naming suggest it maps {subject}"
            if place:
                context_sentence += f" in {place}"
            context_sentence += "."
        elif place:
            context_sentence = f"The file and layer naming suggest it describes features in {place}."

        field_count = int(dataset.column_profile.get("column_count", 0) or 0)
        field_sentence = f"It includes {field_count} non-geometry field(s)." if field_count else ""
        parts = [lead_sentence, context_sentence, field_sentence, support_sentence]
        return " ".join(part for part in parts if part).strip()

    def _geometry_phrase(self, geometry_type: str) -> str:
        geometry = (geometry_type or "").strip().lower()
        if geometry:
            return f"a {geometry} dataset"
        return "a GIS dataset"

    def _subject_phrase(self, theme: str, keywords: list[str]) -> str:
        token_set = set(keywords)
        if theme == "administrative":
            base = "administrative boundaries"
            if any(token in token_set for token in {"district", "distrito", "distritos"}):
                base = "administrative districts"
            elif any(token in token_set for token in {"municipality", "kommune", "county", "province", "region"}):
                base = "administrative areas"
            if any(token in token_set for token in {"coast", "coastal", "costeiro", "costeiros"}):
                base = f"coastal {base}"
            return base
        if theme == "risk":
            if any(token in token_set for token in {"fire", "wildfire", "incendio", "incêndio", "incandio", "queimada", "queimadas"}):
                return "fire risk or wildfire hazard areas"
            if any(token in token_set for token in {"flood", "flooding", "inundacao", "inundação"}):
                return "flood risk or inundation zones"
            if any(token in token_set for token in {"cyclone", "ciclone"}):
                return "cyclone or storm risk areas"
            if any(token in token_set for token in {"drought", "seca"}):
                return "drought risk areas"
            if any(token in token_set for token in {"erosion", "erosao", "erosão"}):
                return "erosion risk areas"
            if any(token in token_set for token in {"seismic", "sismica", "sísmica"}):
                return "seismic hazard areas"
            return "risk or hazard zones"
        if theme == "protected-area":
            if any(token in token_set for token in {"park", "parque"}) and any(token in token_set for token in {"national", "nacional"}):
                return "national park boundaries or management zones"
            if any(token in token_set for token in {"heritage", "patrimonio"}):
                return "protected heritage or conservation areas"
            return "protected areas or conservation zones"
        if theme == "transport":
            return "transport lines or network features"
        if theme == "hydrology":
            if any(token in token_set for token in {"coast", "coastal", "costeiro", "costeiros"}):
                return "coastal or shoreline features"
            return "water-related features"
        if theme == "habitat":
            return "habitat or ecological management areas"
        if theme == "land-use":
            return "land-use, parcel, or zoning features"
        return ""

    def _place_phrase(self, place_names: list[str]) -> str:
        if not place_names:
            return ""
        place = place_names[0].strip()
        if not place:
            return ""
        lowered = place.lower()
        if lowered.startswith("the "):
            return place
        if lowered.endswith((" area", " region", " district", " municipality", " county", " province", " park")):
            return place
        return place

    def _support_sentence(self, candidate: SourceCandidate | None) -> str:
        if candidate is None or candidate.source_type == "placeholder":
            return ""
        if candidate.confidence < 0.2 or not candidate.domain or candidate.domain.endswith(".invalid"):
            return ""
        return f"Search results also point to related source material on {candidate.domain}, which supports that interpretation."

    def _best_descriptive_candidate(self, candidates: list[SourceCandidate]) -> SourceCandidate | None:
        for candidate in candidates:
            if (
                candidate.url
                and candidate.domain
                and not candidate.domain.endswith(".invalid")
                and candidate.source_type != "placeholder"
            ):
                return candidate
        return None

    def _subject_query(self, theme: str, keywords: list[str]) -> str:
        subject = self._subject_phrase(theme, keywords)
        if subject:
            return subject
        return theme if theme != GENERIC_THEME else "gis vector"

    def _dedupe(self, values: list[str], *, limit: int) -> list[str]:
        result: list[str] = []
        seen: set[str] = set()
        for value in values:
            cleaned = str(value).strip()
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            result.append(cleaned)
            if len(result) >= limit:
                break
        return result

    def _group_profile(self, dataset: DatasetRecord) -> dict[str, object]:
        title = dataset.display_name_ai or dataset.preferred_name or _title_case(dataset.layer_name or Path(dataset.source_path).stem)
        tokens = _tokenize(
            " ".join(
                [
                    title,
                    dataset.preferred_description,
                    dataset.geometry_type,
                    Path(dataset.source_path).stem,
                ]
            )
        )
        theme = self._theme_for_tokens(tokens)
        labels: list[str] = []
        if theme != GENERIC_THEME:
            labels.append(_title_case(theme))
        name_label = self._name_based_group_label(title)
        if name_label:
            labels.append(name_label)
        if dataset.suggested_group and dataset.suggested_group != "ungrouped":
            suggested_label = _title_case(dataset.suggested_group)
            if theme == GENERIC_THEME or self._group_similarity(tokens, _tokenize(suggested_label))[0] > 0:
                labels.append(suggested_label)
        if not labels:
            labels.append("Ungrouped")
        return {
            "dataset": dataset,
            "tokens": tokens,
            "theme": theme,
            "labels": self._dedupe(labels, limit=4),
        }

    def _name_based_group_label(self, value: str) -> str:
        tokens = _meaningful_name_tokens(value)
        if not tokens:
            return ""
        return " ".join(token.title() for token in tokens[:2])

    def _group_similarity(self, dataset_tokens: set[str], label_tokens: set[str]) -> tuple[int, int]:
        overlap = len(dataset_tokens.intersection(label_tokens))
        exact_theme_bonus = 1 if overlap and len(label_tokens) == 1 else 0
        return (overlap + exact_theme_bonus, -len(label_tokens))

    def _assign_profiles_to_labels(
        self,
        profiles: list[dict[str, object]],
        target_group_count: int,
        *,
        blocked_labels: set[str] | None = None,
    ) -> dict[str, str]:
        if not profiles:
            return {}
        target = max(1, min(int(target_group_count or 1), len(profiles)))
        base_labels = self._select_base_labels(profiles, target, blocked_labels=blocked_labels)
        if not base_labels:
            base_labels = ["Ungrouped"]
        average_group_size = max(1, (len(profiles) + target - 1) // target)
        group_sizes = {label: 0 for label in base_labels}
        assignments: dict[str, str] = {}
        ordered_profiles = sorted(
            profiles,
            key=lambda profile: (
                str(profile.get("theme") or "") == GENERIC_THEME,
                -len(profile.get("tokens") or set()),
                str(profile["dataset"].preferred_name).lower(),
            ),
        )
        for profile in ordered_profiles:
            profile_labels = self._candidate_labels(profile, blocked_labels=blocked_labels)
            label_positions = {label: index for index, label in enumerate(profile_labels)}
            best_label = base_labels[0]
            best_score: tuple[float, int, int, int, str] | None = None
            theme_label = _title_case(str(profile.get("theme") or ""))
            for label in base_labels:
                similarity, token_penalty = self._group_similarity(profile["tokens"], _tokenize(label))
                direct_bonus = max(0, 8 - (label_positions[label] * 3)) if label in label_positions else 0
                theme_bonus = 4 if theme_label and label.lower() == theme_label.lower() else 0
                size_penalty = int((group_sizes.get(label, 0) / average_group_size) * 4)
                score = (float(direct_bonus + theme_bonus + (similarity * 4) - size_penalty), direct_bonus, similarity, -group_sizes.get(label, 0), label)
                if best_score is None or score > best_score:
                    best_score = score
                    best_label = label
            assignments[profile["dataset"].dataset_id] = best_label
            group_sizes[best_label] = group_sizes.get(best_label, 0) + 1
        return assignments

    def _select_base_labels(
        self,
        profiles: list[dict[str, object]],
        target_group_count: int,
        *,
        blocked_labels: set[str] | None = None,
    ) -> list[str]:
        blocked = {str(label).strip().lower() for label in (blocked_labels or set()) if str(label).strip()}
        target = max(1, min(int(target_group_count or 1), len(profiles)))
        primary_scores: dict[str, int] = {}
        secondary_scores: dict[str, int] = {}
        all_scores: dict[str, int] = {}
        for profile in profiles:
            labels = self._candidate_labels(profile, blocked_labels=blocked)
            for index, label in enumerate(labels):
                all_scores[label] = all_scores.get(label, 0) + max(1, 4 - index)
                if index == 0:
                    primary_scores[label] = primary_scores.get(label, 0) + 3
                else:
                    secondary_scores[label] = secondary_scores.get(label, 0) + max(1, 3 - index)
        if not all_scores and blocked:
            return self._select_base_labels(profiles, target, blocked_labels=None)
        selected: list[str] = []
        primary_quota = target if target <= 2 else max(1, (target + 1) // 2)
        for score_map, quota in ((primary_scores, primary_quota), (secondary_scores, target)):
            for label, _score in sorted(score_map.items(), key=lambda item: (-item[1], item[0])):
                if label in selected:
                    continue
                selected.append(label)
                if len(selected) >= quota:
                    break
            if len(selected) >= target:
                return selected[:target]
        for label, _score in sorted(all_scores.items(), key=lambda item: (-item[1], item[0])):
            if label in selected:
                continue
            selected.append(label)
            if len(selected) >= target:
                break
        return selected[:target]

    def _candidate_labels(self, profile: dict[str, object], *, blocked_labels: set[str] | None = None) -> list[str]:
        blocked = {str(label).strip().lower() for label in (blocked_labels or set()) if str(label).strip()}
        labels = [str(label).strip() for label in profile.get("labels", []) if str(label).strip()]
        filtered = [label for label in labels if label.lower() not in blocked]
        return filtered or labels or ["Ungrouped"]


class OpenAIClassificationProvider(ClassificationProvider, CandidateRanker):
    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        endpoint: str | None = None,
        fallback: ClassificationProvider | None = None,
        session: requests.Session | None = None,
        timeout_s: float | None = None,
        max_consecutive_failures: int | None = None,
        include_source_name: bool = DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME,
        include_layer_name: bool = DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME,
        include_column_names: bool = DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES,
        include_sample_values: bool = DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES,
        include_geometry_type: bool = DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE,
        include_feature_count: bool = DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT,
        include_bbox: bool = DEFAULT_CLASSIFICATION_INCLUDE_BBOX,
    ) -> None:
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "").strip()
        self.model = model or os.environ.get("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
        self.endpoint = endpoint or os.environ.get("OPENAI_ENDPOINT", DEFAULT_OPENAI_ENDPOINT)
        self.fallback = fallback or HeuristicClassificationProvider()
        self.session = session or requests.Session()
        self.timeout_s = float(timeout_s or os.environ.get("OPENAI_TIMEOUT_S", DEFAULT_OPENAI_TIMEOUT_S))
        self.max_consecutive_failures = max(
            1,
            int(max_consecutive_failures or os.environ.get("OPENAI_MAX_CONSECUTIVE_FAILURES", DEFAULT_OPENAI_MAX_CONSECUTIVE_FAILURES)),
        )
        self.consecutive_failures = 0
        self.remote_disabled = False
        self.last_error_message = ""
        self.include_source_name = bool(include_source_name)
        self.include_layer_name = bool(include_layer_name)
        self.include_column_names = bool(include_column_names)
        self.include_sample_values = bool(include_sample_values)
        self.include_geometry_type = bool(include_geometry_type)
        self.include_feature_count = bool(include_feature_count)
        self.include_bbox = bool(include_bbox)

    def remote_availability_status(self) -> tuple[bool, str]:
        if not self.api_key:
            return False, "OpenAI API key is missing. Configure it in Settings to use Find info (AI)."
        if self.remote_disabled:
            if self.last_error_message:
                return False, self.last_error_message
            return False, "OpenAI is unavailable for the rest of this session after repeated failures."
        return True, f"OpenAI is available with model {self.model}."

    def consume_last_error_message(self) -> str:
        message = self.last_error_message.strip()
        self.last_error_message = ""
        return message

    def classify(self, dataset: DatasetRecord) -> DatasetUnderstanding:
        if not self._can_use_remote():
            return self.fallback.classify(dataset)
        payload = self._build_classification_payload(dataset)
        system_prompt = (
            "You classify GIS vector datasets for a desktop catalog using only the supplied metadata summary. "
            "The summary may contain only a subset of clues because the app keeps the first pass token-light. "
            "Prioritize file name, layer name, column names, and sample values when those are present. "
            "Descriptions must be specific and useful, not vague. "
            "Translate or interpret clues such as 'Parque Nacional', 'Distritos Costeiros', or other non-English names when relevant. "
            "Risk or hazard layers such as wildfire risk, flood risk, cyclone risk, drought risk, erosion risk, or seismic risk are not protected areas. "
            "For example, 'Risco de incandio e queimadas extremo' should map to a risk or fire-hazard group, not to protected area. "
            "Make cautious but helpful assumptions from the naming when the evidence is incomplete. "
            "Avoid generic phrases such as 'general geographic'. "
            "Return JSON with keys: theme, keywords, place_names, suggested_title, "
            "suggested_description, suggested_group, search_queries, confidence. "
            "Base the description on the title you infer, the likely subject, and any place clues."
        )
        response = self._chat(system_prompt, json.dumps(payload, ensure_ascii=False))
        if not response:
            return self.fallback.classify(dataset)
        understanding = _parse_understanding(response)
        if understanding is None:
            return self.fallback.classify(dataset)
        return self._merge_with_fallback(dataset, understanding)

    def _build_classification_payload(self, dataset: DatasetRecord) -> dict:
        payload: dict[str, object] = {}
        if self.include_source_name:
            payload["source_name"] = Path(dataset.source_path).stem
        if self.include_layer_name and dataset.layer_name:
            payload["layer_name"] = dataset.layer_name
        column_context = self._build_column_context(dataset.column_profile)
        if column_context:
            payload["columns"] = column_context
        if self.include_geometry_type and dataset.geometry_type:
            payload["geometry_type"] = dataset.geometry_type
        if self.include_feature_count and dataset.feature_count:
            payload["feature_count"] = dataset.feature_count
        if self.include_bbox and dataset.bbox_wgs84:
            payload["bbox_wgs84"] = dataset.bbox_wgs84
        return payload

    def _build_column_context(self, column_profile: dict) -> list[dict[str, object]]:
        context: list[dict[str, object]] = []
        for column in column_profile.get("columns", []):
            if not isinstance(column, dict):
                continue
            entry: dict[str, object] = {}
            name = str(column.get("name") or "").strip()
            if self.include_column_names and name:
                entry["name"] = name
            if self.include_sample_values:
                samples = [
                    str(value).strip()
                    for value in column.get("samples", [])
                    if str(value).strip()
                ][:3]
                if samples:
                    entry["samples"] = samples
            if entry:
                context.append(entry)
        return context

    def enrich_from_sources(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> DatasetUnderstanding:
        fallback_enricher = getattr(self.fallback, "enrich_from_sources", None)
        fallback = fallback_enricher(dataset, understanding, candidates) if callable(fallback_enricher) else self.fallback.classify(dataset)
        if not _has_live_candidates(candidates):
            return fallback
        if not self._can_use_remote() or not candidates:
            return fallback
        payload = {
            "dataset": {
                "source_path": dataset.source_path,
                "source_stem": Path(dataset.source_path).stem,
                "layer_name": dataset.layer_name,
                "default_title_hint": _title_case(dataset.layer_name or Path(dataset.source_path).stem),
                "geometry_type": dataset.geometry_type,
                "feature_count": dataset.feature_count,
                "bbox_wgs84": dataset.bbox_wgs84,
                "column_profile": dataset.column_profile,
            },
            "current_understanding": {
                "theme": understanding.theme,
                "keywords": understanding.keywords,
                "place_names": understanding.place_names,
                "suggested_title": understanding.suggested_title,
                "suggested_description": understanding.suggested_description,
                "suggested_group": understanding.suggested_group,
                "search_queries": understanding.search_queries,
                "confidence": understanding.confidence,
            },
            "search_candidates": [
                {
                    "title": candidate.title,
                    "snippet": candidate.snippet,
                    "domain": candidate.domain,
                    "url": candidate.url,
                    "confidence": candidate.confidence,
                }
                for candidate in candidates[:3]
            ],
        }
        system_prompt = (
            "Improve a GIS dataset understanding using both dataset metadata and corroborating web search results. "
            "The description should synthesize file name, layer name, inferred title, place clues, and relevant search evidence. "
            "Use search results only as supporting evidence; do not invent precise facts that are not supported. "
            "If evidence is partial, say likely or probably. "
            "Avoid vague phrases such as 'general geographic'. "
            "Return JSON with keys: theme, keywords, place_names, suggested_title, suggested_description, suggested_group, search_queries, confidence."
        )
        response = self._chat(system_prompt, json.dumps(payload, ensure_ascii=False))
        if not response:
            return fallback
        improved = _parse_understanding(response)
        if improved is None:
            return fallback
        return self._merge_with_fallback(dataset, improved, fallback_understanding=fallback)

    def group_datasets(
        self,
        datasets: list[DatasetRecord],
        target_group_count: int,
        *,
        timeout_s: float | None = None,
    ) -> dict[str, str]:
        fallback_grouper = getattr(self.fallback, "group_datasets", None)
        fallback = fallback_grouper(datasets, target_group_count, timeout_s=timeout_s) if callable(fallback_grouper) else {}
        if not self._can_use_remote() or len(datasets) <= 1:
            return fallback
        heuristic_profiles = {
            dataset.dataset_id: self.fallback.classify(dataset)
            for dataset in datasets
        }
        payload = {
            "target_group_count": max(1, min(int(target_group_count or 1), len(datasets))),
            "datasets": [
                {
                    "dataset_id": dataset.dataset_id,
                    "name": dataset.preferred_name,
                    "ai_title": dataset.display_name_ai,
                    "description": dataset.preferred_description,
                    "suggested_group": dataset.suggested_group,
                    "local_theme_hint": heuristic_profiles[dataset.dataset_id].theme,
                    "local_group_hint": heuristic_profiles[dataset.dataset_id].suggested_group,
                    "local_keywords": heuristic_profiles[dataset.dataset_id].keywords[:4],
                    "geometry_type": dataset.geometry_type,
                    "source_stem": Path(dataset.source_path).stem,
                }
                for dataset in datasets
            ],
        }
        system_prompt = (
            "Group GIS datasets into a rational set of catalog groups. "
            "Use the requested target_group_count as the intended number of groups. "
            "Translate or interpret non-English titles, descriptions, and group hints when needed before deciding groups. "
            "If datasets use different languages, normalize them by meaning first and group by subject matter, not by shared language. "
            "Treat any existing suggested_group as a hint, not a hard constraint, and override it when the dataset meaning points elsewhere. "
            "Risk or hazard layers such as wildfire risk, flood risk, cyclone risk, drought risk, erosion risk, or seismic risk must not be grouped under Protected Area. "
            "For example, 'Risco de incandio e queimadas extremo' belongs with risk or fire-hazard datasets, not with protected areas. "
            "Avoid catch-all or overly broad groups. "
            "A proposed group should usually stay within roughly three times the average target group size unless every dataset in it is clearly the same subject. "
            "If a tentative group looks mixed or too large, split it into more coherent subgroups. "
            "Prefer short, human-readable group names. "
            "Return JSON with key groups, where each group has name and dataset_ids."
        )
        response = self._chat(system_prompt, json.dumps(payload, ensure_ascii=False), timeout_s=timeout_s)
        if not response:
            return fallback
        try:
            parsed = json.loads(response)
        except json.JSONDecodeError:
            return fallback
        assignments: dict[str, str] = {}
        for group in parsed.get("groups", []):
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name") or "").strip()
            if not group_name:
                continue
            for dataset_id in group.get("dataset_ids", []):
                cleaned_id = str(dataset_id).strip()
                if cleaned_id:
                    assignments[cleaned_id] = group_name
        if not assignments:
            return fallback
        for dataset in datasets:
            if dataset.dataset_id not in assignments and dataset.dataset_id in fallback:
                assignments[dataset.dataset_id] = fallback[dataset.dataset_id]
        repair_helper = self.fallback if isinstance(self.fallback, HeuristicClassificationProvider) else HeuristicClassificationProvider()
        if repair_helper.assignments_look_too_broad(datasets, assignments, target_group_count):
            return repair_helper.group_datasets(datasets, target_group_count, timeout_s=timeout_s)
        return assignments

    def rank(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> list[SourceCandidate]:
        if not _has_live_candidates(candidates):
            return HeuristicCandidateRanker().rank(dataset, understanding, candidates)
        if not self._can_use_remote() or not candidates:
            return HeuristicCandidateRanker().rank(dataset, understanding, candidates)
        prompt = {
            "dataset": {
                "name": dataset.preferred_name,
                "description": understanding.suggested_description,
                "keywords": understanding.keywords,
                "places": understanding.place_names,
            },
            "candidates": [
                {
                    "url": candidate.url,
                    "title": candidate.title,
                    "snippet": candidate.snippet,
                    "domain": candidate.domain,
                }
                for candidate in candidates
            ],
        }
        system_prompt = (
            "Rank search candidates for a GIS dataset. "
            "Return JSON array under key candidates with url, match_reason, confidence, source_type. "
            "Higher confidence means more likely to be the original source or a high quality mirror."
        )
        response = self._chat(system_prompt, json.dumps(prompt, ensure_ascii=False))
        if not response:
            return HeuristicCandidateRanker().rank(dataset, understanding, candidates)
        try:
            payload = json.loads(response)
        except json.JSONDecodeError:
            return HeuristicCandidateRanker().rank(dataset, understanding, candidates)
        ranked = {item.get("url"): item for item in payload.get("candidates", []) if isinstance(item, dict)}
        result: list[SourceCandidate] = []
        for candidate in candidates:
            meta = ranked.get(candidate.url, {})
            result.append(
                SourceCandidate(
                    url=candidate.url,
                    title=candidate.title,
                    snippet=candidate.snippet,
                    domain=candidate.domain,
                    source_type=str(meta.get("source_type") or candidate.source_type or "search-result"),
                    match_reason=str(meta.get("match_reason") or candidate.match_reason or ""),
                    confidence=float(meta.get("confidence") or candidate.confidence or 0),
                    is_selected=False,
                    candidate_id=candidate.candidate_id,
                )
            )
        result.sort(key=lambda item: item.confidence, reverse=True)
        if result:
            result[0].is_selected = True
        return result

    def _merge_with_fallback(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        fallback_understanding: DatasetUnderstanding | None = None,
    ) -> DatasetUnderstanding:
        fallback = fallback_understanding or self.fallback.classify(dataset)
        return _merge_understandings(understanding, fallback)

    def _can_use_remote(self) -> bool:
        if not self.api_key:
            self.last_error_message = "OpenAI API key is missing. Configure it in Settings to use Find info (AI)."
            return False
        if self.remote_disabled:
            if not self.last_error_message:
                self.last_error_message = "OpenAI is unavailable for the rest of this session after repeated failures."
            return False
        return True

    def _chat(self, system_prompt: str, user_prompt: str, *, timeout_s: float | None = None) -> str:
        if not self._can_use_remote():
            return ""
        try:
            effective_timeout_s = self.timeout_s if timeout_s is None else max(0.1, min(self.timeout_s, float(timeout_s)))
            response = self.session.post(
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "temperature": 0.2,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "response_format": {"type": "json_object"},
                },
                timeout=effective_timeout_s,
            )
            response.raise_for_status()
            envelope = response.json()
            self.consecutive_failures = 0
            self.last_error_message = ""
            return str(envelope["choices"][0]["message"]["content"]).strip()
        except requests.Timeout:
            self.last_error_message = f"OpenAI request timed out after {effective_timeout_s:g}s."
            self.consecutive_failures += 1
        except requests.HTTPError as exc:
            self.last_error_message = self._http_error_message(exc)
            self.consecutive_failures += 1
        except requests.RequestException as exc:
            self.last_error_message = f"OpenAI request failed: {exc.__class__.__name__}: {exc}"
            self.consecutive_failures += 1
        except Exception as exc:
            self.last_error_message = f"OpenAI request failed: {exc.__class__.__name__}: {exc}"
            self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.remote_disabled = True
            self.last_error_message = (
                f"{self.last_error_message} "
                "Remote AI has been disabled for the rest of this session."
            ).strip()
        return ""

    def _http_error_message(self, exc: requests.HTTPError) -> str:
        response = exc.response
        if response is None:
            return f"OpenAI request failed: HTTP error: {exc}"
        status_code = int(getattr(response, "status_code", 0) or 0)
        error_message = ""
        error_code = ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                error = payload.get("error", {})
                if isinstance(error, dict):
                    error_message = str(error.get("message") or "").strip()
                    error_code = str(error.get("code") or "").strip().lower()
        except Exception:
            error_message = ""
        if status_code == 401:
            return "OpenAI request failed with 401 Unauthorized. Check the API key in Settings."
        if status_code == 429:
            if error_code == "insufficient_quota" or "quota" in error_message.lower():
                return "OpenAI request failed because the account has no remaining quota."
            return "OpenAI request hit a 429 rate limit. Slow down requests or try again later."
        if status_code >= 500:
            return f"OpenAI request failed with server error {status_code}. Try again later."
        if error_message:
            return f"OpenAI request failed with HTTP {status_code}: {error_message}"
        return f"OpenAI request failed with HTTP {status_code}."


class DuckDuckGoSearchProvider(SourceSearchProvider):
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        timeout_s: float | None = None,
        max_consecutive_failures: int | None = None,
        target_candidates: int | None = None,
    ) -> None:
        self.session = session or requests.Session()
        self.timeout_s = float(timeout_s or _env_with_legacy("GRASP_SEARCH_TIMEOUT_S", "MESA_SEARCH_TIMEOUT_S", DEFAULT_SEARCH_TIMEOUT_S))
        self.max_consecutive_failures = max(
            1,
            int(
                max_consecutive_failures
                or _env_with_legacy(
                    "GRASP_SEARCH_MAX_CONSECUTIVE_FAILURES",
                    "MESA_SEARCH_MAX_CONSECUTIVE_FAILURES",
                    DEFAULT_SEARCH_MAX_CONSECUTIVE_FAILURES,
                )
            ),
        )
        self.target_candidates = max(
            1,
            int(
                target_candidates
                or _env_with_legacy(
                    "GRASP_SEARCH_TARGET_CANDIDATES",
                    "MESA_SEARCH_TARGET_CANDIDATES",
                    DEFAULT_SEARCH_TARGET_CANDIDATES,
                )
            ),
        )
        self.consecutive_failures = 0
        self.remote_disabled = False

    def search(self, queries: Iterable[str]) -> list[SourceCandidate]:
        if self.remote_disabled:
            return []
        candidates: list[SourceCandidate] = []
        seen_urls: set[str] = set()
        for query in queries:
            query_text = str(query).strip()
            if not query_text:
                continue
            if len(candidates) >= self.target_candidates:
                break
            try:
                response = self.session.get(
                    f"https://duckduckgo.com/html/?q={quote(query_text)}",
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=self.timeout_s,
                )
                response.raise_for_status()
            except Exception:
                self.consecutive_failures += 1
                if self.consecutive_failures >= self.max_consecutive_failures:
                    self.remote_disabled = True
                    break
                continue
            self.consecutive_failures = 0
            for candidate in self._parse_candidates(response.text):
                if not candidate.url or candidate.url in seen_urls:
                    continue
                seen_urls.add(candidate.url)
                candidates.append(candidate)
                if len(candidates) >= self.target_candidates:
                    break
        return candidates

    def _parse_candidates(self, html: str) -> list[SourceCandidate]:
        pattern = re.compile(
            r'<a[^>]+class="result__a"[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>.*?'
            r'<a[^>]+class="result__snippet"[^>]*>(?P<snippet>.*?)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        results: list[SourceCandidate] = []
        for match in pattern.finditer(html):
            url = _strip_html(match.group("href"))
            title = _strip_html(match.group("title"))
            snippet = _strip_html(match.group("snippet"))
            domain = urlparse(url).netloc
            if not url or not title:
                continue
            results.append(
                SourceCandidate(
                    url=url,
                    title=title,
                    snippet=snippet,
                    domain=domain,
                    source_type="search-result",
                    confidence=0.0,
                )
            )
        return results


class HeuristicSearchProvider(SourceSearchProvider):
    def search(self, queries: Iterable[str]) -> list[SourceCandidate]:
        candidates: list[SourceCandidate] = []
        for index, query in enumerate(queries, start=1):
            query_text = str(query).strip()
            if not query_text:
                continue
            url = f"https://search.example.invalid/query/{index}?q={quote(query_text)}"
            candidates.append(
                SourceCandidate(
                    url=url,
                    title=f"Search placeholder for {query_text}",
                    snippet="No network search provider available. This placeholder preserves the query intent.",
                    domain="search.example.invalid",
                    source_type="placeholder",
                    confidence=0.0,
                )
            )
        return candidates


class HeuristicCandidateRanker(CandidateRanker):
    def rank(
        self,
        dataset: DatasetRecord,
        understanding: DatasetUnderstanding,
        candidates: list[SourceCandidate],
    ) -> list[SourceCandidate]:
        scored: list[SourceCandidate] = []
        dataset_tokens = _tokenize(
            " ".join(
                [
                    dataset.preferred_name,
                    understanding.theme,
                    " ".join(understanding.keywords),
                    " ".join(understanding.place_names),
                ]
            )
        )
        for candidate in candidates:
            haystack = _tokenize(" ".join([candidate.title, candidate.snippet, candidate.domain]))
            overlap = len(dataset_tokens.intersection(haystack))
            domain = candidate.domain.lower()
            if candidate.source_type == "placeholder" or domain.endswith(".invalid"):
                scored.append(
                    SourceCandidate(
                        url=candidate.url,
                        title=candidate.title,
                        snippet=candidate.snippet,
                        domain=candidate.domain,
                        source_type="placeholder",
                        match_reason="Placeholder query preserved because no live search results were available.",
                        confidence=0.0,
                        is_selected=False,
                        candidate_id=candidate.candidate_id,
                    )
                )
                continue
            trust_bonus = 0.2 if any(marker in domain for marker in (".gov", ".kommune", ".county", ".no", ".org")) else 0.0
            confidence = min(0.95, 0.25 + overlap * 0.08 + trust_bonus)
            source_type = "official" if trust_bonus else "search-result"
            reason = (
                f"Matched {overlap} keyword(s) between dataset profile and candidate text."
                if overlap
                else "Low-overlap candidate kept as a fallback search result."
            )
            scored.append(
                SourceCandidate(
                    url=candidate.url,
                    title=candidate.title,
                    snippet=candidate.snippet,
                    domain=candidate.domain,
                    source_type=source_type,
                    match_reason=reason,
                    confidence=round(confidence, 2),
                    is_selected=False,
                    candidate_id=candidate.candidate_id,
                )
            )
        scored.sort(key=lambda item: item.confidence, reverse=True)
        deduped: list[SourceCandidate] = []
        seen_urls: set[str] = set()
        for candidate in scored:
            if candidate.url in seen_urls or not candidate.url:
                continue
            seen_urls.add(candidate.url)
            deduped.append(candidate)
            if len(deduped) >= 5:
                break
        if deduped:
            deduped[0].is_selected = True
        return deduped


def _parse_understanding(payload: str) -> DatasetUnderstanding | None:
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None
    try:
        return DatasetUnderstanding(
            theme=str(data.get("theme") or ""),
            keywords=[str(value) for value in data.get("keywords", [])][:8],
            place_names=[str(value) for value in data.get("place_names", [])][:5],
            suggested_title=str(data.get("suggested_title") or ""),
            suggested_description=str(data.get("suggested_description") or ""),
            suggested_group=str(data.get("suggested_group") or ""),
            search_queries=[str(value) for value in data.get("search_queries", [])][:3],
            confidence=float(data.get("confidence") or 0),
        )
    except Exception:
        return None


def _strip_html(value: str) -> str:
    return re.sub(r"<[^>]+>", "", value).replace("&amp;", "&").strip()


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[^A-Za-z0-9\u00E6\u00F8\u00E5\u00C6\u00D8\u00C5]+", value.lower()) if len(token) > 2}


def _title_case(value: str) -> str:
    cleaned = re.sub(r"[_\\-]+", " ", value).strip()
    return cleaned.title() if cleaned else "Untitled Dataset"


def _meaningful_name_tokens(value: str) -> list[str]:
    stopwords = {
        "data",
        "dataset",
        "layer",
        "table",
        "map",
        "geojson",
        "parquet",
        "shape",
        "shp",
        "gpkg",
        "file",
    }
    tokens: list[str] = []
    for token in re.split(r"[^A-Za-z0-9]+", value.lower()):
        if len(token) <= 2 or token in stopwords:
            continue
        if token not in tokens:
            tokens.append(token)
    return tokens[:4]


def _tokenize(value: str) -> set[str]:
    normalized = value.replace("_", " ")
    return {
        token
        for token in re.split(r"[^\w\u00C0-\u017F]+", normalized.lower(), flags=re.UNICODE)
        if len(token) > 2 and not token.isdigit()
    }


def _meaningful_name_tokens(value: str) -> list[str]:
    stopwords = {
        "data",
        "dataset",
        "layer",
        "table",
        "map",
        "geojson",
        "parquet",
        "shape",
        "shp",
        "gpkg",
        "file",
    }
    tokens: list[str] = []
    for token in re.split(r"[^\w\u00C0-\u017F]+", value.lower(), flags=re.UNICODE):
        if len(token) <= 2 or token in stopwords:
            continue
        if token not in tokens:
            tokens.append(token)
    return tokens[:4]


def _description_is_generic(value: str) -> bool:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        return True
    return any(marker in cleaned for marker in GENERIC_DESCRIPTION_MARKERS)


def _merge_understandings(primary: DatasetUnderstanding, fallback: DatasetUnderstanding) -> DatasetUnderstanding:
    primary.theme = primary.theme or fallback.theme
    primary.keywords = _merge_unique(primary.keywords, fallback.keywords, limit=8)
    primary.place_names = _merge_unique(primary.place_names, fallback.place_names, limit=5)
    primary.suggested_title = primary.suggested_title or fallback.suggested_title
    if _description_is_generic(primary.suggested_description):
        primary.suggested_description = fallback.suggested_description
    if not primary.suggested_group or primary.suggested_group.strip() in {"", "general-geographic", "ungrouped"}:
        primary.suggested_group = fallback.suggested_group
    primary.search_queries = _merge_unique(primary.search_queries, fallback.search_queries, limit=3)
    if primary.confidence <= 0:
        primary.confidence = fallback.confidence
    return primary


def _merge_unique(primary: list[str], fallback: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in [*primary, *fallback]:
        cleaned = str(value).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
        if len(result) >= limit:
            break
    return result


def _has_live_candidates(candidates: list[SourceCandidate]) -> bool:
    for candidate in candidates:
        domain = str(candidate.domain or "").strip().lower()
        if not candidate.url:
            continue
        if candidate.source_type == "placeholder":
            continue
        if not domain or domain.endswith(".invalid"):
            continue
        return True
    return False


def _env_with_legacy(primary_key: str, legacy_key: str, default):
    return os.environ.get(primary_key) or os.environ.get(legacy_key, default)

