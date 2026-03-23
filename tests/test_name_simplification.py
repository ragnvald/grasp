from __future__ import annotations

import unittest

from grasp.name_simplification import suggest_simplified_dataset_name


class NameSimplificationTests(unittest.TestCase):
    def test_hierarchical_source_name_uses_last_segment_and_keeps_context_in_description(self) -> None:
        suggestion = suggest_simplified_dataset_name(
            "cartografia_tematica__toponimia__posto_fronteira__posto_fronteira",
            source_kind="layer",
        )

        self.assertIsNotNone(suggestion)
        self.assertEqual(suggestion.display_name, "Posto fronteira")
        self.assertIn("Cartografia tematica > Toponimia", suggestion.description_note)
        self.assertIn("Original source layer name", suggestion.description_note)

    def test_long_file_name_without_hierarchy_uses_short_tail_name(self) -> None:
        suggestion = suggest_simplified_dataset_name(
            "ministry_transport_infrastructure_national_primary_secondary_corridors.geojson",
            source_kind="file",
        )

        self.assertIsNotNone(suggestion)
        self.assertEqual(suggestion.display_name, "National primary secondary corridors")
        self.assertIn("Original source file name", suggestion.description_note)
        self.assertIn(".geojson", suggestion.description_note)

    def test_short_readable_name_is_left_unchanged(self) -> None:
        self.assertIsNone(suggest_simplified_dataset_name("Roads", source_kind="layer"))


if __name__ == "__main__":
    unittest.main()
