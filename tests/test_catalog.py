from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from grasp.catalog.repository import CatalogRepository
from grasp.models import DatasetRecord, DatasetUnderstanding, SourceCandidate


class CatalogRepositoryTests(unittest.TestCase):
    def test_dataset_default_name_humanizes_imported_source_file_name(self) -> None:
        dataset = DatasetRecord(
            dataset_id="parks",
            source_path="D:/data/nationalparks.shp",
            source_format="shp",
        )

        self.assertEqual(dataset.source_basename, "nationalparks.shp")
        self.assertEqual(dataset.default_name, "National parks")
        self.assertEqual(dataset.preferred_name, "National parks")

    def test_dataset_default_name_preserves_explicit_layer_name(self) -> None:
        dataset = DatasetRecord(
            dataset_id="parks-layer",
            source_path="D:/data/bundle.gpkg",
            source_format="gpkg",
            layer_name="Protected Area",
        )

        self.assertEqual(dataset.default_name, "Protected Area")

    def test_repository_persists_datasets_understandings_and_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            dataset = DatasetRecord(
                dataset_id="ds1",
                source_path="D:/data/roads.geojson",
                source_format="geojson",
                geometry_type="LineString",
                feature_count=2,
                column_profile_json='{"columns":[]}',
                fingerprint="abc",
                cache_path="cache/ds1.parquet",
            )
            repo.replace_datasets([dataset])
            repo.save_dataset_user_fields("ds1", display_name_user="Roads", description_user="User text", visibility=True, include_in_export=True)

            understanding = DatasetUnderstanding(
                theme="transport",
                keywords=["road"],
                suggested_title="Road network",
                suggested_description="AI description",
                suggested_group="transport",
                search_queries=["road dataset"],
                confidence=0.8,
            )
            repo.upsert_understanding("ds1", understanding)
            repo.apply_suggested_group("ds1")

            sources = [
                SourceCandidate(
                    url="https://example.org/roads",
                    title="Official roads",
                    domain="example.org",
                    match_reason="Keyword overlap",
                    confidence=0.91,
                    is_selected=True,
                    candidate_id="src1",
                )
            ]
            repo.replace_sources("ds1", sources)

            stored = repo.get_dataset("ds1")
            self.assertIsNotNone(stored)
            self.assertEqual(stored.preferred_name, "Roads")
            self.assertEqual(stored.group_id, "transport")

            stored_understanding = repo.get_understanding("ds1")
            self.assertEqual(stored_understanding.suggested_title, "Road network")

            stored_sources = repo.list_sources("ds1")
            self.assertEqual(len(stored_sources), 1)
            self.assertTrue(stored_sources[0].is_selected)

    def test_replace_sources_selects_highest_confidence_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="ds1",
                        source_path="D:/data/roads.geojson",
                        source_format="geojson",
                        cache_path="cache/ds1.parquet",
                    )
                ]
            )

            repo.replace_sources(
                "ds1",
                [
                    SourceCandidate(
                        url="https://example.org/roads",
                        title="Official roads",
                        confidence=0.91,
                        candidate_id="src1",
                    ),
                    SourceCandidate(
                        url="https://mirror.example.org/roads",
                        title="Mirror roads",
                        confidence=0.67,
                        candidate_id="src2",
                    ),
                ],
            )

            stored_sources = repo.list_sources("ds1")
            self.assertEqual(len(stored_sources), 2)
            self.assertTrue(stored_sources[0].is_selected)
            self.assertFalse(stored_sources[1].is_selected)

    def test_replace_sources_breaks_top_confidence_ties_randomly(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="ds1",
                        source_path="D:/data/roads.geojson",
                        source_format="geojson",
                        cache_path="cache/ds1.parquet",
                    )
                ]
            )

            with patch("grasp.catalog.repository.random.choice", return_value=1):
                repo.replace_sources(
                    "ds1",
                    [
                        SourceCandidate(
                            url="https://example.org/roads",
                            title="Official roads",
                            confidence=0.91,
                            candidate_id="src1",
                        ),
                        SourceCandidate(
                            url="https://mirror.example.org/roads",
                            title="Mirror roads",
                            confidence=0.91,
                            candidate_id="src2",
                        ),
                    ],
                )

            stored_sources = repo.list_sources("ds1")
            self.assertEqual(len(stored_sources), 2)
            self.assertEqual(stored_sources[0].candidate_id, "src2")
            self.assertTrue(stored_sources[0].is_selected)
            self.assertFalse(stored_sources[1].is_selected)

    def test_update_ordering_moves_datasets_between_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.create_group("Group A")
            repo.create_group("Group B")
            repo.replace_datasets(
                [
                    DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", group_id="group-a", cache_path="a.parquet"),
                    DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", group_id="group-a", cache_path="b.parquet"),
                ]
            )
            repo.update_ordering(["ungrouped", "group-a", "group-b"], [("a", "group-b", 0), ("b", "group-a", 0)])
            a = repo.get_dataset("a")
            b = repo.get_dataset("b")
            self.assertEqual(a.group_id, "group-b")
            self.assertEqual(b.sort_order, 0)

    def test_replace_datasets_reuses_unchanged_ai_and_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            original = DatasetRecord(
                dataset_id="ds1",
                source_path="D:/data/roads.geojson",
                source_format="geojson",
                source_mtime_ns=10,
                source_size_bytes=100,
                geometry_type="LineString",
                feature_count=2,
                column_profile_json='{"columns":[]}',
                fingerprint="same",
                cache_path="data_out/cache/datasets/ds1.parquet",
            )
            repo.replace_datasets([original])
            repo.upsert_understanding(
                "ds1",
                DatasetUnderstanding(
                    suggested_title="Road network",
                    suggested_description="AI text",
                    suggested_group="transport",
                    confidence=0.9,
                ),
            )
            repo.replace_sources(
                "ds1",
                [
                    SourceCandidate(
                        url="https://example.org/roads",
                        title="Roads",
                        confidence=0.8,
                        is_selected=True,
                        candidate_id="src1",
                    )
                ],
            )

            summary = repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="ds1",
                        source_path="D:/data/roads.geojson",
                        source_format="geojson",
                        source_mtime_ns=10,
                        source_size_bytes=100,
                        geometry_type="LineString",
                        feature_count=2,
                        column_profile_json='{"columns":[]}',
                        fingerprint="same",
                        cache_path="data_out/cache/datasets/ds1.parquet",
                    )
                ]
            )

            self.assertEqual(summary["changed_ids"], [])
            self.assertEqual(summary["reused_ids"], ["ds1"])
            self.assertEqual(repo.get_dataset("ds1").display_name_ai, "Road network")
            self.assertEqual(len(repo.list_sources("ds1")), 1)

    def test_bulk_visibility_updates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.create_group("Transport")
            repo.replace_datasets(
                [
                    DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", group_id="transport", cache_path="a.parquet"),
                    DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", group_id="transport", cache_path="b.parquet"),
                    DatasetRecord(dataset_id="c", source_path="c", source_format="geojson", group_id="ungrouped", cache_path="c.parquet"),
                ]
            )

            repo.set_visibility_for_group("transport", False)
            self.assertFalse(repo.get_dataset("a").visibility)
            self.assertFalse(repo.get_dataset("b").visibility)
            self.assertTrue(repo.get_dataset("c").visibility)

            repo.set_visibility_for_datasets(["a", "c"], True)
            self.assertTrue(repo.get_dataset("a").visibility)
            self.assertFalse(repo.get_dataset("b").visibility)
            self.assertTrue(repo.get_dataset("c").visibility)

    def test_assign_groups_bulk_reassigns_and_prunes_empty_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.create_group("Old Group")
            repo.replace_datasets(
                [
                    DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", group_id="old-group", cache_path="a.parquet"),
                    DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", group_id="old-group", cache_path="b.parquet"),
                ]
            )

            changed = repo.assign_groups_bulk({"a": "Administrative", "b": "Protected Area"})

            self.assertEqual(changed, 2)
            self.assertEqual(repo.get_dataset("a").group_id, "administrative")
            self.assertEqual(repo.get_dataset("b").group_id, "protected-area")
            self.assertNotIn(("old-group", "Old Group"), repo.list_groups())

    def test_create_group_reuses_equivalent_plural_variant(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")

            first_group_id = repo.create_group("Protected Area")
            second_group_id = repo.create_group("Protected Areas")

            self.assertEqual(first_group_id, "protected-area")
            self.assertEqual(second_group_id, "protected-area")
            self.assertEqual(repo.list_groups().count(("protected-area", "Protected Area")), 1)

    def test_assign_groups_bulk_merges_singular_plural_group_variants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", group_id="ungrouped", cache_path="a.parquet"),
                    DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", group_id="ungrouped", cache_path="b.parquet"),
                ]
            )

            changed = repo.assign_groups_bulk({"a": "Protected Area", "b": "Protected Areas"})

            self.assertEqual(changed, 2)
            self.assertEqual(repo.get_dataset("a").group_id, "protected-area")
            self.assertEqual(repo.get_dataset("b").group_id, "protected-area")
            self.assertEqual(repo.list_groups().count(("protected-area", "Protected Area")), 1)

    def test_reset_groups_moves_datasets_to_ungrouped_and_prunes_empty_groups(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.create_group("Old Group")
            repo.create_group("Keep Group")
            repo.replace_datasets(
                [
                    DatasetRecord(dataset_id="a", source_path="a", source_format="geojson", group_id="old-group", cache_path="a.parquet"),
                    DatasetRecord(dataset_id="b", source_path="b", source_format="geojson", group_id="old-group", cache_path="b.parquet"),
                    DatasetRecord(dataset_id="c", source_path="c", source_format="geojson", group_id="keep-group", cache_path="c.parquet"),
                ]
            )

            changed = repo.reset_groups(["a", "b"])

            self.assertEqual(changed, 2)
            self.assertEqual(repo.get_dataset("a").group_id, "ungrouped")
            self.assertEqual(repo.get_dataset("b").group_id, "ungrouped")
            self.assertEqual(repo.get_dataset("c").group_id, "keep-group")
            self.assertNotIn(("old-group", "Old Group"), repo.list_groups())
            self.assertIn(("keep-group", "Keep Group"), repo.list_groups())

    def test_upsert_understandings_bulk_updates_ai_fields_and_auto_assigns_group(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="a",
                        source_path="D:/data/a.geojson",
                        source_format="geojson",
                        group_id="ungrouped",
                        cache_path="a.parquet",
                    ),
                    DatasetRecord(
                        dataset_id="b",
                        source_path="D:/data/b.geojson",
                        source_format="geojson",
                        group_id="manual-group",
                        cache_path="b.parquet",
                    ),
                ]
            )

            changed = repo.upsert_understandings_bulk(
                [
                    (
                        "a",
                        DatasetUnderstanding(
                            suggested_title="Protected Area",
                            suggested_description="Auto description A",
                            suggested_group="protected-area",
                            confidence=0.8,
                        ),
                    ),
                    (
                        "b",
                        DatasetUnderstanding(
                            suggested_title="Administrative",
                            suggested_description="Auto description B",
                            suggested_group="administrative",
                            confidence=0.7,
                        ),
                    ),
                ],
                auto_assign_group=True,
            )

            self.assertEqual(changed, 2)
            self.assertEqual(repo.get_dataset("a").display_name_ai, "Protected Area")
            self.assertEqual(repo.get_dataset("a").group_id, "protected-area")
            self.assertEqual(repo.get_dataset("b").display_name_ai, "Administrative")
            self.assertEqual(repo.get_dataset("b").group_id, "manual-group")
            self.assertIn(("protected-area", "Protected Area"), repo.list_groups())

    def test_upsert_understandings_bulk_without_auto_assign_keeps_groups_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="a",
                        source_path="D:/data/a.geojson",
                        source_format="geojson",
                        group_id="ungrouped",
                        cache_path="a.parquet",
                    )
                ]
            )

            changed = repo.upsert_understandings_bulk(
                [
                    (
                        "a",
                        DatasetUnderstanding(
                            suggested_title="Coastal Buffer",
                            suggested_description="Auto description",
                            suggested_group="coastal",
                            confidence=0.8,
                        ),
                    )
                ],
                auto_assign_group=False,
            )

            stored = repo.get_dataset("a")
            self.assertEqual(changed, 1)
            self.assertEqual(stored.display_name_ai, "Coastal Buffer")
            self.assertEqual(stored.suggested_group, "coastal")
            self.assertEqual(stored.group_id, "ungrouped")
            self.assertNotIn(("coastal", "Coastal"), repo.list_groups())

    def test_repository_persists_source_style_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo = CatalogRepository(Path(tmp) / "catalog.sqlite")
            repo.replace_datasets(
                [
                    DatasetRecord(
                        dataset_id="styled",
                        source_path="D:/data/roads.geojson",
                        source_format="geojson",
                        source_style_summary="Possible source styling detected: QGIS QML style file (roads.qml).",
                        source_style_items_json='[{"kind":"sidecar:qml","label":"QGIS QML style file (roads.qml)","path":"D:/data/roads.qml"}]',
                        cache_path="styled.parquet",
                    )
                ]
            )

            stored = repo.get_dataset("styled")

            self.assertIsNotNone(stored)
            self.assertTrue(stored.has_source_style)
            self.assertIn("roads.qml", stored.source_style_summary)
            self.assertEqual(len(stored.source_style_items), 1)
            self.assertEqual(stored.source_style_items[0]["kind"], "sidecar:qml")


if __name__ == "__main__":
    unittest.main()

