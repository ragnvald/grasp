from __future__ import annotations

from dataclasses import dataclass

from grasp.branding import (
    APP_ORGANIZATION,
    APP_SETTINGS_APP,
    LEGACY_SETTINGS_APP,
    LEGACY_SETTINGS_ORGANIZATION,
)
from grasp.intelligence.providers import (
    DEFAULT_CLASSIFICATION_INCLUDE_BBOX,
    DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES,
    DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT,
    DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE,
    DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME,
    DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES,
    DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME,
    DEFAULT_OPENAI_ENDPOINT,
    DEFAULT_OPENAI_MAX_CONSECUTIVE_FAILURES,
    DEFAULT_OPENAI_MODEL,
    DEFAULT_OPENAI_TIMEOUT_S,
    DEFAULT_SEARCH_MAX_CONSECUTIVE_FAILURES,
    DEFAULT_SEARCH_TARGET_CANDIDATES,
    DEFAULT_SEARCH_TIMEOUT_S,
)
from grasp.qt_compat import QSettings


@dataclass(slots=True)
class AppSettings:
    openai_model: str = DEFAULT_OPENAI_MODEL
    openai_api_key: str = ""
    openai_endpoint: str = DEFAULT_OPENAI_ENDPOINT
    openai_timeout_s: float = DEFAULT_OPENAI_TIMEOUT_S
    openai_max_consecutive_failures: int = DEFAULT_OPENAI_MAX_CONSECUTIVE_FAILURES
    classification_include_source_name: bool = DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME
    classification_include_layer_name: bool = DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME
    classification_include_column_names: bool = DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES
    classification_include_sample_values: bool = DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES
    classification_include_geometry_type: bool = DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE
    classification_include_feature_count: bool = DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT
    classification_include_bbox: bool = DEFAULT_CLASSIFICATION_INCLUDE_BBOX
    search_timeout_s: float = DEFAULT_SEARCH_TIMEOUT_S
    search_max_consecutive_failures: int = DEFAULT_SEARCH_MAX_CONSECUTIVE_FAILURES
    search_target_candidates: int = DEFAULT_SEARCH_TARGET_CANDIDATES
    last_folder: str = ""


class SettingsStore:
    def __init__(self, settings_backend: QSettings | None = None) -> None:
        self._settings = settings_backend or QSettings(APP_ORGANIZATION, APP_SETTINGS_APP)
        self._legacy_settings = None if settings_backend is not None else QSettings(
            LEGACY_SETTINGS_ORGANIZATION,
            LEGACY_SETTINGS_APP,
        )

    def load(self) -> AppSettings:
        return AppSettings(
            openai_model=str(self._value("openai/model", DEFAULT_OPENAI_MODEL)),
            openai_api_key=str(self._value("openai/api_key", "")),
            openai_endpoint=str(self._value("openai/endpoint", DEFAULT_OPENAI_ENDPOINT)),
            openai_timeout_s=float(self._value("openai/timeout_s", DEFAULT_OPENAI_TIMEOUT_S)),
            openai_max_consecutive_failures=int(
                self._value(
                    "openai/max_consecutive_failures",
                    DEFAULT_OPENAI_MAX_CONSECUTIVE_FAILURES,
                )
            ),
            classification_include_source_name=_to_bool(
                self._value("openai/classification_include_source_name", DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME),
                DEFAULT_CLASSIFICATION_INCLUDE_SOURCE_NAME,
            ),
            classification_include_layer_name=_to_bool(
                self._value("openai/classification_include_layer_name", DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME),
                DEFAULT_CLASSIFICATION_INCLUDE_LAYER_NAME,
            ),
            classification_include_column_names=_to_bool(
                self._value("openai/classification_include_column_names", DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES),
                DEFAULT_CLASSIFICATION_INCLUDE_COLUMN_NAMES,
            ),
            classification_include_sample_values=_to_bool(
                self._value("openai/classification_include_sample_values", DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES),
                DEFAULT_CLASSIFICATION_INCLUDE_SAMPLE_VALUES,
            ),
            classification_include_geometry_type=_to_bool(
                self._value("openai/classification_include_geometry_type", DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE),
                DEFAULT_CLASSIFICATION_INCLUDE_GEOMETRY_TYPE,
            ),
            classification_include_feature_count=_to_bool(
                self._value("openai/classification_include_feature_count", DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT),
                DEFAULT_CLASSIFICATION_INCLUDE_FEATURE_COUNT,
            ),
            classification_include_bbox=_to_bool(
                self._value("openai/classification_include_bbox", DEFAULT_CLASSIFICATION_INCLUDE_BBOX),
                DEFAULT_CLASSIFICATION_INCLUDE_BBOX,
            ),
            search_timeout_s=float(self._value("search/timeout_s", DEFAULT_SEARCH_TIMEOUT_S)),
            search_max_consecutive_failures=int(
                self._value(
                    "search/max_consecutive_failures",
                    DEFAULT_SEARCH_MAX_CONSECUTIVE_FAILURES,
                )
            ),
            search_target_candidates=int(
                self._value(
                    "search/target_candidates",
                    DEFAULT_SEARCH_TARGET_CANDIDATES,
                )
            ),
            last_folder=str(self._value("ui/last_folder", "")),
        )

    def save(self, settings: AppSettings) -> None:
        self._settings.setValue("openai/model", settings.openai_model.strip() or DEFAULT_OPENAI_MODEL)
        self._settings.setValue("openai/api_key", settings.openai_api_key.strip())
        self._settings.setValue("openai/endpoint", settings.openai_endpoint.strip() or DEFAULT_OPENAI_ENDPOINT)
        self._settings.setValue("openai/timeout_s", float(settings.openai_timeout_s))
        self._settings.setValue(
            "openai/max_consecutive_failures",
            int(max(1, settings.openai_max_consecutive_failures)),
        )
        self._settings.setValue("openai/classification_include_source_name", bool(settings.classification_include_source_name))
        self._settings.setValue("openai/classification_include_layer_name", bool(settings.classification_include_layer_name))
        self._settings.setValue("openai/classification_include_column_names", bool(settings.classification_include_column_names))
        self._settings.setValue("openai/classification_include_sample_values", bool(settings.classification_include_sample_values))
        self._settings.setValue("openai/classification_include_geometry_type", bool(settings.classification_include_geometry_type))
        self._settings.setValue("openai/classification_include_feature_count", bool(settings.classification_include_feature_count))
        self._settings.setValue("openai/classification_include_bbox", bool(settings.classification_include_bbox))
        self._settings.setValue("search/timeout_s", float(settings.search_timeout_s))
        self._settings.setValue(
            "search/max_consecutive_failures",
            int(max(1, settings.search_max_consecutive_failures)),
        )
        self._settings.setValue(
            "search/target_candidates",
            int(max(1, settings.search_target_candidates)),
        )
        self._settings.setValue("ui/last_folder", settings.last_folder.strip())
        self._settings.sync()

    def _value(self, key: str, default):
        value = self._settings.value(key, None)
        if value is None and self._legacy_settings is not None:
            value = self._legacy_settings.value(key, None)
        if value is None:
            return default
        return value


def _to_bool(value, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)

