from __future__ import annotations

import json
import unittest

import requests

from grasp.intelligence.providers import (
    DEFAULT_OPENAI_MODEL,
    DuckDuckGoSearchProvider,
    HeuristicCandidateRanker,
    HeuristicClassificationProvider,
    HeuristicSearchProvider,
    OpenAIClassificationProvider,
    _has_live_candidates,
)
from grasp.intelligence.service import SearchService
from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate


class _FailingSession:
    def __init__(self) -> None:
        self.calls = 0

    def post(self, *_args, **_kwargs):
        self.calls += 1
        raise requests.RequestException("boom")


class _CapturingResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _CapturingSession:
    def __init__(self, payload: dict) -> None:
        self.calls = 0
        self.last_timeout = None
        self.payload = payload

    def post(self, *_args, **kwargs):
        self.calls += 1
        self.last_timeout = kwargs.get("timeout")
        return _CapturingResponse(self.payload)


class _HttpErrorResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _FailingSearchSession:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        raise requests.RequestException("boom")


class IntelligenceTests(unittest.TestCase):
    def test_heuristic_classification_builds_queries_and_group(self) -> None:
        dataset = DatasetRecord(
            dataset_id="roads",
            source_path="D:/data/road_network.geojson",
            source_format="geojson",
            geometry_type="LineString",
            feature_count=12,
            column_profile_json=json.dumps(
                {
                    "columns": [
                        {"name": "road_name", "dtype": "object", "samples": ["E6"]},
                        {"name": "kommune", "dtype": "object", "samples": ["Trondheim"]},
                    ],
                    "column_count": 2,
                }
            ),
        )
        provider = HeuristicClassificationProvider()
        understanding = provider.classify(dataset)
        self.assertEqual(understanding.suggested_group, "transport")
        self.assertEqual(len(understanding.search_queries), 3)
        self.assertIn("Trondheim", understanding.place_names)

    def test_search_service_ranks_candidates(self) -> None:
        dataset = DatasetRecord(
            dataset_id="roads",
            source_path="D:/data/road_network.geojson",
            source_format="geojson",
            display_name_ai="Road Network",
            geometry_type="LineString",
            feature_count=12,
        )
        search = SearchService(provider=HeuristicSearchProvider(), ranker=HeuristicCandidateRanker())
        understanding = HeuristicClassificationProvider().classify(dataset)
        candidates = search.find_sources(understanding, dataset)
        self.assertTrue(candidates)
        self.assertTrue(candidates[0].is_selected)
        self.assertEqual(candidates[0].confidence, 0.0)

    def test_general_group_uses_dataset_name_tokens(self) -> None:
        dataset = DatasetRecord(
            dataset_id="protected-areas",
            source_path="D:/data/protected_areas_coastal.geojson",
            source_format="geojson",
            geometry_type="Polygon",
            feature_count=8,
            column_profile_json=json.dumps({"columns": [], "column_count": 0}),
        )
        provider = HeuristicClassificationProvider()
        understanding = provider.classify(dataset)
        self.assertEqual(understanding.suggested_group, "protected-area")

    def test_heuristic_description_uses_name_clues_not_generic_theme(self) -> None:
        dataset = DatasetRecord(
            dataset_id="districts",
            source_path="D:/data/administrativo_simplified_distritos_costeiros.geojson",
            source_format="geojson",
            layer_name="Administrativo Simplified Distritos Costeiros",
            geometry_type="Polygon",
            feature_count=18,
            column_profile_json=json.dumps({"columns": [], "column_count": 0}),
        )
        provider = HeuristicClassificationProvider()
        understanding = provider.classify(dataset)

        self.assertEqual(understanding.theme, "administrative")
        self.assertIn("coastal administrative districts", understanding.suggested_description.lower())
        self.assertNotIn("general geographic", understanding.suggested_description.lower())

    def test_source_enrichment_sharpens_description_from_search_candidates(self) -> None:
        dataset = DatasetRecord(
            dataset_id="park",
            source_path="D:/data/aase_patrimonio3_parque_nacional.geojson",
            source_format="geojson",
            layer_name="Aase Patrimonio3 Parque Nacional",
            geometry_type="Polygon",
            feature_count=7,
            column_profile_json=json.dumps({"columns": [], "column_count": 0}),
        )
        provider = HeuristicClassificationProvider()
        understanding = DatasetUnderstanding(
            theme="protected-area",
            keywords=["aase", "patrimonio3", "parque", "nacional"],
            place_names=["Aase Patrimonio3"],
            suggested_title="Aase Patrimonio3 Parque Nacional",
            suggested_description="Aase Patrimonio3 Parque Nacional appears to relate to general geographic data.",
            suggested_group="protected-area",
            search_queries=["Aase Patrimonio3 Parque Nacional geodata"],
            confidence=0.4,
        )

        enriched = provider.enrich_from_sources(
            dataset,
            understanding,
            [
                SourceCandidate(
                    url="https://parks.example.org/aase-patrimonio3",
                    title="Parque Nacional Aase Patrimonio3 official boundary",
                    snippet="Protected area boundary and management information for the national park.",
                    domain="parks.example.org",
                    source_type="official",
                    confidence=0.81,
                )
            ],
        )

        self.assertIn("national park", enriched.suggested_description.lower())
        self.assertIn("supports that interpretation", enriched.suggested_description.lower())
        self.assertNotIn("general geographic", enriched.suggested_description.lower())

    def test_placeholder_search_domain_is_not_used_in_description_support_text(self) -> None:
        dataset = DatasetRecord(
            dataset_id="buffer",
            source_path="D:/data/aase_buffer_faixa_costeira.geojson",
            source_format="geojson",
            layer_name="Aase Buffer Faixa Costeira",
            geometry_type="MultiPolygon",
            feature_count=8,
            column_profile_json=json.dumps({"columns": [], "column_count": 10}),
        )
        provider = HeuristicClassificationProvider()
        understanding = provider.classify(dataset)
        placeholder_candidates = HeuristicCandidateRanker().rank(
            dataset,
            understanding,
            HeuristicSearchProvider().search(understanding.search_queries),
        )

        enriched = provider.enrich_from_sources(dataset, understanding, placeholder_candidates)

        self.assertNotIn("search.example.invalid", enriched.suggested_description.lower())
        self.assertNotIn("supports that interpretation", enriched.suggested_description.lower())

    def test_default_openai_model_constant(self) -> None:
        self.assertEqual(DEFAULT_OPENAI_MODEL, "gpt-4o-mini")

    def test_duckduckgo_search_disables_remote_after_first_failed_request(self) -> None:
        provider = DuckDuckGoSearchProvider(
            session=_FailingSearchSession(),
            timeout_s=0.01,
            max_consecutive_failures=1,
        )

        candidates = provider.search(["one", "two", "three"])

        self.assertEqual(candidates, [])
        self.assertTrue(provider.remote_disabled)
        self.assertEqual(provider.consecutive_failures, 1)
        self.assertEqual(provider.session.calls, 1)

    def test_live_candidate_detection_ignores_placeholder_domains(self) -> None:
        self.assertFalse(
            _has_live_candidates(
                [
                    SourceCandidate(
                        url="https://search.example.invalid/query/1?q=test",
                        title="Search placeholder",
                        domain="search.example.invalid",
                        source_type="placeholder",
                    )
                ]
            )
        )
        self.assertTrue(
            _has_live_candidates(
                [
                    SourceCandidate(
                        url="https://example.org/dataset",
                        title="Dataset",
                        domain="example.org",
                        source_type="search-result",
                    )
                ]
            )
        )

    def test_group_datasets_respects_target_group_count(self) -> None:
        provider = HeuristicClassificationProvider()
        assignments = provider.group_datasets(
            [
                DatasetRecord(dataset_id="a", source_path="D:/data/admin_districts.geojson", source_format="geojson", layer_name="Administrativo Distritos"),
                DatasetRecord(dataset_id="b", source_path="D:/data/admin_capital.geojson", source_format="geojson", layer_name="Administrativo Capital"),
                DatasetRecord(dataset_id="c", source_path="D:/data/patrimonio_parque.geojson", source_format="geojson", layer_name="Patrimonio Parque Nacional"),
                DatasetRecord(dataset_id="d", source_path="D:/data/patrimonio_reserva.geojson", source_format="geojson", layer_name="Patrimonio Reserva Nacional"),
            ],
            2,
        )

        self.assertEqual(len(assignments), 4)
        self.assertEqual(len(set(assignments.values())), 2)

    def test_openai_provider_disables_remote_after_repeated_failures(self) -> None:
        dataset = DatasetRecord(
            dataset_id="zones",
            source_path="D:/data/zones.geojson",
            source_format="geojson",
            layer_name="conservacao_bazaruto_uso_limitado",
            geometry_type="MultiPolygon",
            feature_count=5,
            column_profile_json=json.dumps(
                {
                    "columns": [
                        {"name": "descr", "dtype": "object", "samples": ["Zona de Uso Limitado"]},
                        {"name": "fonte", "dtype": "object", "samples": ["ANAC (2016)"]},
                    ],
                    "column_count": 2,
                }
            ),
        )
        session = _FailingSession()
        provider = OpenAIClassificationProvider(
            api_key="test-key",
            fallback=HeuristicClassificationProvider(),
            session=session,
            timeout_s=0.01,
            max_consecutive_failures=2,
        )

        first = provider.classify(dataset)
        second = provider.classify(dataset)
        third = provider.classify(dataset)

        self.assertTrue(first.suggested_title)
        self.assertTrue(second.suggested_title)
        self.assertTrue(third.suggested_title)
        self.assertTrue(provider.remote_disabled)
        self.assertEqual(session.calls, 2)

    def test_openai_provider_reports_missing_api_key_status(self) -> None:
        provider = OpenAIClassificationProvider()

        available, message = provider.remote_availability_status()

        self.assertFalse(available)
        self.assertIn("OpenAI API key is missing", message)

    def test_openai_provider_consumes_last_error_message_once(self) -> None:
        provider = OpenAIClassificationProvider()
        provider.last_error_message = "OpenAI request timed out after 20s."

        message = provider.consume_last_error_message()

        self.assertEqual(message, "OpenAI request timed out after 20s.")
        self.assertEqual(provider.consume_last_error_message(), "")

    def test_openai_http_error_message_reports_missing_quota(self) -> None:
        provider = OpenAIClassificationProvider(api_key="test-key")
        response = _HttpErrorResponse(
            429,
            {
                "error": {
                    "message": "You exceeded your current quota.",
                    "code": "insufficient_quota",
                }
            },
        )

        message = provider._http_error_message(requests.HTTPError(response=response))

        self.assertIn("no remaining quota", message)

    def test_openai_http_error_message_reports_rate_limit(self) -> None:
        provider = OpenAIClassificationProvider(api_key="test-key")
        response = _HttpErrorResponse(
            429,
            {
                "error": {
                    "message": "Rate limit reached for requests per min.",
                    "code": "rate_limit_exceeded",
                }
            },
        )

        message = provider._http_error_message(requests.HTTPError(response=response))

        self.assertIn("429 rate limit", message)

    def test_openai_http_error_message_reports_unauthorized_key(self) -> None:
        provider = OpenAIClassificationProvider(api_key="bad-key")
        response = _HttpErrorResponse(401, {"error": {"message": "Invalid authentication credentials"}})

        message = provider._http_error_message(requests.HTTPError(response=response))

        self.assertIn("401 Unauthorized", message)

    def test_openai_group_datasets_uses_timeout_override(self) -> None:
        session = _CapturingSession(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "groups": [
                                        {"name": "Administrative", "dataset_ids": ["a", "b"]},
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
        )
        provider = OpenAIClassificationProvider(
            api_key="test-key",
            fallback=HeuristicClassificationProvider(),
            session=session,
            timeout_s=20.0,
        )

        assignments = provider.group_datasets(
            [
                DatasetRecord(dataset_id="a", source_path="D:/data/a.geojson", source_format="geojson", layer_name="Administrativo Distritos"),
                DatasetRecord(dataset_id="b", source_path="D:/data/b.geojson", source_format="geojson", layer_name="Administrativo Capital"),
            ],
            2,
            timeout_s=7.5,
        )

        self.assertEqual(assignments, {"a": "Administrative", "b": "Administrative"})
        self.assertEqual(session.calls, 1)
        self.assertEqual(session.last_timeout, 7.5)

    def test_openai_initial_classification_payload_is_token_light_by_default(self) -> None:
        dataset = DatasetRecord(
            dataset_id="zones",
            source_path="D:/data/zones.geojson",
            source_format="geojson",
            layer_name="Conservacao Borealis",
            geometry_type="MultiPolygon",
            feature_count=27,
            bbox_wgs84=[31.0, -24.0, 32.0, -23.0],
            column_profile_json=json.dumps(
                {
                    "columns": [
                        {"name": "zone_name", "dtype": "object", "samples": ["Zona A", "Zona B"]},
                        {"name": "status", "dtype": "object", "samples": ["Protected"]},
                    ]
                }
            ),
        )
        provider = OpenAIClassificationProvider(api_key="test-key")

        payload = provider._build_classification_payload(dataset)

        self.assertEqual(payload["source_name"], "zones")
        self.assertEqual(payload["layer_name"], "Conservacao Borealis")
        self.assertEqual(payload["columns"], [{"name": "zone_name"}, {"name": "status"}])
        self.assertNotIn("geometry_type", payload)
        self.assertNotIn("feature_count", payload)
        self.assertNotIn("bbox_wgs84", payload)

    def test_openai_initial_classification_payload_can_include_optional_context(self) -> None:
        dataset = DatasetRecord(
            dataset_id="zones",
            source_path="D:/data/zones.geojson",
            source_format="geojson",
            layer_name="Conservacao Borealis",
            geometry_type="MultiPolygon",
            feature_count=27,
            bbox_wgs84=[31.0, -24.0, 32.0, -23.0],
            column_profile_json=json.dumps(
                {
                    "columns": [
                        {"name": "zone_name", "dtype": "object", "samples": ["Zona A", "Zona B"]},
                    ]
                }
            ),
        )
        provider = OpenAIClassificationProvider(
            api_key="test-key",
            include_sample_values=True,
            include_geometry_type=True,
            include_feature_count=True,
            include_bbox=True,
        )

        payload = provider._build_classification_payload(dataset)

        self.assertEqual(payload["columns"], [{"name": "zone_name", "samples": ["Zona A", "Zona B"]}])
        self.assertEqual(payload["geometry_type"], "MultiPolygon")
        self.assertEqual(payload["feature_count"], 27)
        self.assertEqual(payload["bbox_wgs84"], [31.0, -24.0, 32.0, -23.0])


if __name__ == "__main__":
    unittest.main()

