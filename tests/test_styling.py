from __future__ import annotations

import unittest
from pathlib import Path

from grasp.models import DatasetRecord
from grasp.styling import StyleService


class StyleServiceTests(unittest.TestCase):
    def test_style_service_uses_dataset_name_and_description(self) -> None:
        service = StyleService()
        dataset = DatasetRecord(
            dataset_id="ds1",
            source_path="D:/data/parque_nacional.gpkg",
            source_format="gpkg",
            layer_name="Patrimonio Parque Nacional",
            description_ai="Protected national park boundaries for a heritage conservation area.",
            geometry_type="MultiPolygon",
        )

        style = service.style_for_dataset(dataset, group_name="Protected Areas")

        self.assertEqual(style.theme, "protected-area")
        self.assertGreater(style.fill_opacity, 0.0)
        self.assertIn("Protected Area", style.label)

    def test_qgis_project_xml_references_exported_layers(self) -> None:
        service = StyleService()

        xml = service.qgis_project_xml(
            project_name="GRASP Export",
            gpkg_path=Path("D:/exports/grasp_package.gpkg"),
            layer_specs=[
                {
                    "dataset_id": "roads",
                    "display_name": "Roads",
                    "description": "Transport lines",
                    "layer_name": "Roads",
                    "geometry_type": "Line",
                    "style_summary": "Line styling inferred from transport naming.",
                    "style_theme": "transport",
                }
            ],
            bounds=[10.0, 63.0, 11.0, 64.0],
        )

        self.assertIn("grasp_package.gpkg|layername=Roads", xml)
        self.assertIn("grasp/style_theme", xml)


if __name__ == "__main__":
    unittest.main()

