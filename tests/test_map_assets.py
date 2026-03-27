from __future__ import annotations

import unittest
from pathlib import Path


class MapAssetTests(unittest.TestCase):
    def test_leaflet_asset_includes_layer_extent_control(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('createBrowserButton("Extent"', html)
        self.assertIn('modeLabel.textContent = "Map mode";', html)
        self.assertIn('createBrowserButton("One layer at a time"', html)
        self.assertIn('createBrowserButton("Show all selected layers"', html)
        self.assertIn("function zoomToCurrentLayerExtent()", html)

    def test_leaflet_asset_allows_layer_list_to_be_hidden(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('createMapControlButton("Show Layers"', html)
        self.assertIn("function toggleLayerControlVisibility()", html)
        self.assertIn("function hideLayerControlVisibility()", html)
        self.assertIn("function ensureLayerControlDismissUi(container)", html)
        self.assertIn('closeButton.textContent = "Close";', html)
        self.assertIn('layerListButton.textContent = layerListVisible ? "Hide Layers" : "Show Layers";', html)
        self.assertIn('.leaflet-control-layers .grasp-layer-close {', html)
        self.assertIn(".leaflet-control-layers.grasp-layers-hidden {", html)

    def test_leaflet_asset_caps_show_all_mode_for_safety(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("const MAX_VISIBLE_LAYER_RENDER_COUNT = 24;", html)
        self.assertIn('availableDatasets.slice(0, MAX_VISIBLE_LAYER_RENDER_COUNT)', html)
        self.assertIn('Loaded first " + loaded.visibleLayers.length + " of " + loaded.visibleCount + " available dataset(s) (safety limit).', html)

    def test_leaflet_asset_handles_resize_for_leaflet_and_offline_modes(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("map.invalidateSize(false);", html)
        self.assertIn("window.addEventListener(\"resize\", handleViewportResize);", html)
        self.assertIn("function currentMapCanvasSize()", html)

    def test_leaflet_asset_refits_when_active_dataset_scope_changes(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("let lastLeafletAutoFitKey = \"\";", html)
        self.assertIn("function leafletAutoFitKey(state, datasets)", html)
        self.assertIn("function shouldAutoFitLeaflet(state, datasets)", html)
        self.assertIn("if (shouldAutoFitLeaflet(state, loaded.datasets)){", html)

    def test_leaflet_asset_supports_optional_state_payload_for_manual_profiling(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("async function reloadState(rawState)", html)
        self.assertIn('const raw = typeof queuedRawState === "string" ? queuedRawState : await bridge.getState();', html)
        self.assertIn("bridge.stateChanged.connect(function(rawState){", html)
        self.assertIn("reloadState();", html)
        self.assertIn("let reloadInFlight = false;", html)

    def test_leaflet_asset_includes_bridge_and_parse_timing_logs(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn("function logTiming(label, startedMs, details)", html)
        self.assertIn('"bridge.getLayerGeoJson()"', html)
        self.assertIn('"JSON.parse(geojson)"', html)
        self.assertIn('"reloadState total"', html)

    def test_leaflet_asset_reports_runtime_errors_to_console(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn('window.addEventListener("error", function(event){', html)
        self.assertIn('window.addEventListener("unhandledrejection", function(event){', html)
        self.assertIn('"[MapError] Unhandled rejection: "', html)

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

    def test_leaflet_asset_breaks_long_popup_words(self) -> None:
        html = self._leaflet_map_html()
        self.assertIn(".offline-popup {", html)
        self.assertIn(".leaflet-popup-content {", html)
        self.assertIn(".dataset-popup .popup-description {", html)
        self.assertIn(".dataset-popup .popup-attributes div {", html)
        self.assertIn("overflow-wrap: anywhere;", html)
        self.assertIn("word-break: break-word;", html)

    def _leaflet_map_html(self) -> str:
        path = Path("D:/code/codex_mesa/src/grasp/ui/assets/leaflet_map.html")
        return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
