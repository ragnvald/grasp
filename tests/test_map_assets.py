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

    def _leaflet_map_html(self) -> str:
        path = Path("D:/code/codex_mesa/src/grasp/ui/assets/leaflet_map.html")
        return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
