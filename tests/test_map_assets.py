from __future__ import annotations

import unittest
from pathlib import Path


class MapAssetTests(unittest.TestCase):
    def test_leaflet_asset_includes_layer_extent_control(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('createBrowserButton("Extent"', html)
        self.assertIn("function zoomToCurrentLayerExtent()", html)

    def test_leaflet_asset_handles_resize_for_leaflet_and_offline_modes(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("map.invalidateSize(false);", html)
        self.assertIn("window.addEventListener(\"resize\", handleViewportResize);", html)
        self.assertIn("function currentMapCanvasSize()", html)

    def test_leaflet_asset_uses_structured_popup_markup(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('lines.push(\'<div class="dataset-popup">\');', html)
        self.assertIn('lines.push(\'<div class="popup-title">\' + escapeHtml(dataset.name || "Dataset") + "</div>");', html)
        self.assertIn('lines.push(\'<div class="popup-description">\' + escapeHtml(dataset.description) + "</div>");', html)
        self.assertIn('lines.push(\'<div class="popup-attributes">\');', html)

    def test_leaflet_asset_uses_explicit_popup_typography_styles(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('.leaflet-popup-content-wrapper {', html)
        self.assertIn('.leaflet-popup-content {', html)
        self.assertIn('font-family: "Segoe UI Variable Text", "Segoe UI", "Noto Sans", sans-serif;', html)
        self.assertIn("font-size: 14px;", html)
        self.assertIn("text-rendering: optimizeLegibility;", html)

    def _leaflet_map_html(self) -> str:
        path = Path("D:/code/codex_mesa/src/grasp/ui/assets/leaflet_map.html")
        return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
