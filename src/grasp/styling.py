from __future__ import annotations

from html import escape
from pathlib import Path
import re
from typing import Iterable
from xml.etree import ElementTree as ET

from grasp.models import DatasetRecord, LayerStyle


class StyleService:
    PALETTES = {
        "administrative": ("#2f6690", "#8fb3cc"),
        "hydrology": ("#2a6f97", "#79b4d8"),
        "risk": ("#b02a37", "#f5b7b1"),
        "transport": ("#b85c38", "#f0b27a"),
        "protected-area": ("#4d7c0f", "#a8c686"),
        "habitat": ("#588157", "#b7d3a8"),
        "land-use": ("#8c6a43", "#d9ba8a"),
        "heritage": ("#7c5535", "#d4a373"),
        "coastal": ("#0f766e", "#83c5be"),
        "general": ("#6c757d", "#ced4da"),
    }
    THEME_HINTS = {
        "administrative": {"admin", "administrativo", "district", "distrito", "province", "boundary", "capital"},
        "hydrology": {
            "water",
            "river",
            "rivers",
            "rio",
            "rios",
            "lake",
            "lakes",
            "hydro",
            "shore",
            "coast",
            "costeiro",
            "costeira",
            "costeiros",
            "agua",
            "aguas",
            "água",
            "águas",
            "ribeira",
            "ribeiras",
            "arroyo",
            "arroyos",
            "canal",
            "canais",
            "drenagem",
        },
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
        "transport": {"road", "street", "route", "transport", "rail", "track", "bridge", "linha"},
        "protected-area": {"protected", "park", "parque", "reserve", "reserva", "conservacao", "conservation"},
        "habitat": {"habitat", "ecology", "forest", "wetland", "mangrove", "species"},
        "land-use": {"parcel", "landuse", "agricultura", "agriculture", "building", "zoning", "ocupacoes"},
        "heritage": {"heritage", "patrimonio", "historic", "historia", "cultural", "fort", "farol"},
        "coastal": {"coast", "coastal", "costeiro", "costeira", "costeiros", "shoreline", "marinho"},
    }

    def style_for_dataset(self, dataset: DatasetRecord, *, group_name: str = "") -> LayerStyle:
        theme = self._theme_for_dataset(dataset, group_name=group_name)
        stroke_color, fill_color = self.PALETTES.get(theme, self.PALETTES["general"])
        geometry = self._geometry_category(dataset.geometry_type)
        label = self._style_label(theme, geometry)
        summary = self._style_summary(dataset, theme, geometry)
        if geometry == "polygon":
            return LayerStyle(
                label=label,
                summary=summary,
                theme=theme,
                stroke_color=stroke_color,
                fill_color=fill_color,
                fill_opacity=0.22,
                stroke_width=1.35,
                line_opacity=0.92,
                point_radius=6.0,
                point_stroke_color="#fffdf8",
                point_stroke_width=1.2,
                point_fill_opacity=0.9,
            )
        if geometry == "line":
            return LayerStyle(
                label=label,
                summary=summary,
                theme=theme,
                stroke_color=stroke_color,
                fill_color=fill_color,
                fill_opacity=0.0,
                stroke_width=2.6,
                line_opacity=0.96,
                point_radius=6.0,
                point_stroke_color="#fffdf8",
                point_stroke_width=1.2,
                point_fill_opacity=0.9,
            )
        return LayerStyle(
            label=label,
            summary=summary,
            theme=theme,
            stroke_color=stroke_color,
            fill_color=fill_color,
            fill_opacity=0.24,
            stroke_width=1.2,
            line_opacity=0.96,
            point_radius=6.5,
            point_stroke_color="#fffdf8",
            point_stroke_width=1.25,
            point_fill_opacity=0.92,
        )

    def qgis_style_qml(self, dataset: DatasetRecord, style: LayerStyle) -> str:
        geometry = self._geometry_category(dataset.geometry_type)
        renderer_type = {
            "polygon": "fill",
            "line": "line",
            "point": "marker",
        }.get(geometry, "fill")
        symbol_layer_class = {
            "polygon": "SimpleFill",
            "line": "SimpleLine",
            "point": "SimpleMarker",
        }.get(geometry, "SimpleFill")
        prop_map = self._qgis_symbol_properties(style, geometry)
        option_entries = "\n".join(
            f'            <Option name="{escape(name)}" type="QString" value="{escape(value)}"/>'
            for name, value in prop_map.items()
        )
        prop_entries = "\n".join(
            f'        <prop k="{escape(name)}" v="{escape(value)}"/>'
            for name, value in prop_map.items()
        )
        alpha = f"{self._symbol_alpha(style, geometry):.2f}"
        return (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            "<qgis styleCategories=\"Symbology\" version=\"3.34.0\">\n"
            f"  <renderer-v2 type=\"singleSymbol\" symbollevels=\"0\" referencescale=\"-1\" forceraster=\"0\" enableorderby=\"0\">\n"
            "    <symbols>\n"
            f"      <symbol alpha=\"{alpha}\" clip_to_extent=\"1\" type=\"{renderer_type}\" name=\"0\" force_rhr=\"0\">\n"
            f"        <layer class=\"{symbol_layer_class}\" enabled=\"1\" locked=\"0\" pass=\"0\">\n"
            "          <Option type=\"Map\">\n"
            f"{option_entries}\n"
            "          </Option>\n"
            f"{prop_entries}\n"
            "        </layer>\n"
            "      </symbol>\n"
            "    </symbols>\n"
            "  </renderer-v2>\n"
            "</qgis>\n"
        )

    def qgis_project_xml(
        self,
        *,
        project_name: str,
        data_source: str,
        layer_specs: list[dict[str, str]],
        bounds: list[float],
        provider_key: str = "ogr",
    ) -> str:
        project = ET.Element(
            "qgis",
            {
                "projectname": project_name,
                "version": "3.34.0",
            },
        )
        title = ET.SubElement(project, "title")
        title.text = project_name

        layer_tree = ET.SubElement(project, "layer-tree-group", {"name": project_name, "checked": "Qt::Checked"})
        project_layers = ET.SubElement(project, "projectlayers")
        for spec in layer_specs:
            layer_name = spec.get("layer_name", "")
            datasource = data_source
            if layer_name:
                datasource = f"{data_source}|layername={layer_name}"
            subset_string = spec.get("subset_string", "")
            layer_id = spec["dataset_id"]
            ET.SubElement(
                layer_tree,
                "layer-tree-layer",
                {
                    "id": layer_id,
                    "name": spec["display_name"],
                    "source": datasource,
                    "providerKey": provider_key,
                    "checked": "Qt::Checked",
                },
            )
            maplayer = ET.SubElement(project_layers, "maplayer", {"type": "vector", "geometry": spec["geometry_type"]})
            ET.SubElement(maplayer, "id").text = layer_id
            ET.SubElement(maplayer, "layername").text = spec["display_name"]
            ET.SubElement(maplayer, "datasource").text = datasource
            ET.SubElement(maplayer, "provider").text = provider_key
            ET.SubElement(maplayer, "shortname").text = spec["display_name"]
            ET.SubElement(maplayer, "title").text = spec["display_name"]
            ET.SubElement(maplayer, "abstract").text = spec.get("description", "")
            if subset_string:
                ET.SubElement(maplayer, "subsetString").text = subset_string
            custom = ET.SubElement(maplayer, "customproperties")
            ET.SubElement(custom, "property", {"key": "grasp/style_summary", "value": spec.get("style_summary", "")})
            ET.SubElement(custom, "property", {"key": "grasp/style_theme", "value": spec.get("style_theme", "")})
            style_qml = spec.get("style_qml", "")
            if style_qml:
                try:
                    style_root = ET.fromstring(style_qml)
                except ET.ParseError:
                    style_root = None
                if style_root is not None:
                    renderer = style_root.find("renderer-v2")
                    if renderer is not None:
                        maplayer.append(renderer)

        canvas = ET.SubElement(project, "mapcanvas")
        extent = ET.SubElement(canvas, "extent")
        normalized_bounds = bounds if len(bounds) == 4 else [-180.0, -90.0, 180.0, 90.0]
        for tag, value in zip(("xmin", "ymin", "xmax", "ymax"), normalized_bounds):
            ET.SubElement(extent, tag).text = str(value)
        ET.SubElement(canvas, "rotation").text = "0"

        tree = ET.ElementTree(project)
        return ET.tostring(tree.getroot(), encoding="unicode")

    def _theme_for_dataset(self, dataset: DatasetRecord, *, group_name: str) -> str:
        text = " ".join(
            [
                dataset.preferred_name,
                dataset.preferred_description,
                dataset.display_name_ai,
                dataset.description_ai,
                dataset.suggested_group,
                dataset.group_id,
                group_name,
            ]
        )
        tokens = self._tokens(text)
        for theme, hints in self.THEME_HINTS.items():
            if tokens.intersection(hints):
                return theme
        return "general"

    def _style_label(self, theme: str, geometry: str) -> str:
        theme_label = theme.replace("-", " ").title()
        geometry_label = {
            "polygon": "Polygon",
            "line": "Line",
            "point": "Point",
        }.get(geometry, "Vector")
        return f"{theme_label} {geometry_label}"

    def _style_summary(self, dataset: DatasetRecord, theme: str, geometry: str) -> str:
        geometry_phrase = {
            "polygon": "semi-transparent polygon styling",
            "line": "line styling",
            "point": "point styling",
        }.get(geometry, "vector styling")
        return (
            f"{geometry_phrase.title()} inferred from the dataset name and description for the "
            f"{theme.replace('-', ' ')} theme."
        )

    def _geometry_category(self, geometry_type: str) -> str:
        value = str(geometry_type or "").lower()
        if "point" in value:
            return "point"
        if "line" in value:
            return "line"
        if "polygon" in value:
            return "polygon"
        return "other"

    def _tokens(self, value: str) -> set[str]:
        return {token for token in re.split(r"[^\w\u00C0-\u017F]+", value.lower(), flags=re.UNICODE) if len(token) > 2}

    def _qgis_symbol_properties(self, style: LayerStyle, geometry: str) -> dict[str, str]:
        if geometry == "point":
            return {
                "name": "circle",
                "color": style.fill_color,
                "outline_color": style.point_stroke_color,
                "outline_width": f"{style.point_stroke_width:.2f}",
                "size": f"{style.point_radius * 0.62:.2f}",
                "size_unit": "MM",
            }
        if geometry == "line":
            return {
                "capstyle": "round",
                "joinstyle": "round",
                "line_color": style.stroke_color,
                "line_width": f"{style.stroke_width:.2f}",
                "line_width_unit": "MM",
            }
        return {
            "color": style.fill_color,
            "outline_color": style.stroke_color,
            "outline_width": f"{style.stroke_width:.2f}",
            "outline_width_unit": "MM",
            "style": "solid",
        }

    def _symbol_alpha(self, style: LayerStyle, geometry: str) -> float:
        if geometry == "point":
            return max(0.05, min(1.0, style.point_fill_opacity))
        if geometry == "line":
            return max(0.05, min(1.0, style.line_opacity))
        return max(0.05, min(1.0, style.fill_opacity))


def merge_bounds(bounds_list: Iterable[list[float]]) -> list[float]:
    usable = [bounds for bounds in bounds_list if isinstance(bounds, list) and len(bounds) == 4]
    if not usable:
        return []
    minx = min(float(bounds[0]) for bounds in usable)
    miny = min(float(bounds[1]) for bounds in usable)
    maxx = max(float(bounds[2]) for bounds in usable)
    maxy = max(float(bounds[3]) for bounds in usable)
    return [minx, miny, maxx, maxy]

