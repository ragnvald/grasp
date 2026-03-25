from __future__ import annotations

import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

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

    def test_style_service_recognizes_rios_as_hydrology(self) -> None:
        service = StyleService()
        dataset = DatasetRecord(
            dataset_id="ds2",
            source_path="D:/data/rios.gpkg",
            source_format="gpkg",
            layer_name="Rios",
            geometry_type="MultiLineString",
        )

        style = service.style_for_dataset(dataset, group_name="")

        self.assertEqual(style.theme, "hydrology")
        self.assertEqual(style.stroke_color, StyleService.PALETTES["hydrology"][0])

    def test_style_service_recognizes_fire_risk_as_risk(self) -> None:
        service = StyleService()
        dataset = DatasetRecord(
            dataset_id="ds3",
            source_path="D:/data/risco_incandio_queimadas_extremo.gpkg",
            source_format="gpkg",
            layer_name="Risco de incandio e queimadas extremo",
            geometry_type="MultiPolygon",
        )

        style = service.style_for_dataset(dataset, group_name="")

        self.assertEqual(style.theme, "risk")
        self.assertEqual(style.stroke_color, StyleService.PALETTES["risk"][0])

    def test_qgis_project_xml_references_exported_layers(self) -> None:
        service = StyleService()

        xml = service.qgis_project_xml(
            project_name="GRASP Export",
            data_source="grasp_package.gpkg",
            layer_specs=[
                {
                    "dataset_id": "roads",
                    "display_name": "Roads",
                    "description": "Transport lines",
                    "layer_name": "Roads",
                    "geometry_type": "Line",
                    "style_summary": "Line styling inferred from transport naming.",
                    "style_theme": "transport",
                    "subset_string": "\"dataset_id\" = 'roads'",
                }
            ],
            bounds=[10.0, 63.0, 11.0, 64.0],
        )

        self.assertIn("grasp_package.gpkg|layername=Roads", xml)
        self.assertIn("grasp/style_theme", xml)
        root = ET.fromstring(xml)
        self.assertEqual(root.findtext(".//projectlayers/maplayer/subsetString"), "\"dataset_id\" = 'roads'")


if __name__ == "__main__":
    unittest.main()

