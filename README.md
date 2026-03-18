# GRASP

GRASP stands for **Geo-data Retrieval, Analysis, Styling and Packaging**.

GRASP is a desktop application for finding GIS vector datasets in a user-selected folder, building structured metadata about them, enriching that metadata with AI and source discovery, grouping datasets into sensible collections, previewing them in a lightweight map, and exporting a packaged deliverable.

The project is authored by **Ragnvald Larsen**.

## What GRASP does

GRASP is designed to help a user go from a messy folder of mixed geodata files to a more structured, reviewable, and exportable catalog.

Core workflow:

1. Scan a folder recursively for supported GIS vector formats.
2. Profile the datasets and cache normalized working copies.
3. Build AI-assisted names, descriptions, and grouping hints.
4. Find likely external sources for the datasets.
5. Let the user review, group, order, and edit the catalog.
6. Preview the data in a desktop map.
7. Generate styling heuristics from names and descriptions.
8. Export a packaged GeoPackage or GeoParquet deliverable.

## Supported formats

Input:

- Shapefile (`.shp`)
- GeoPackage (`.gpkg`, including multiple layers)
- GeoJSON (`.geojson`)
- GeoJSON-like JSON (`.json`)
- GeoParquet / Parquet (`.parquet`)

Output:

- GeoPackage
- GeoParquet
- GeoPackage sidecar QGIS project (`.qgs`)

## Technology

- Python 3.11+
- `PySide6` for the desktop UI
- `QWebEngineView` with Leaflet for map preview
- `geopandas`, `fiona`, `shapely`, `pyarrow` for ingest, geometry handling, and export
- SQLite for local project/catalog state

## Project workspace

When a folder is opened in GRASP, the app creates a local working area under `data_out/` inside that folder.

Typical layout:

```text
data_out/
  catalog.sqlite
  cache/
    datasets/
  exports/
  logs/
  temp/
  log.txt
```

Raw input datasets are treated as read-only. GRASP works against cached and derived data inside `data_out/`.

## Local development

Create a virtual environment and install the project:

```powershell
python -m venv .venv
. .\.venv\Scripts\Activate.ps1
pip install -e .[dev]
```

Run the app:

```powershell
grasp
```

Windows launcher:

```powershell
.\run_app.cmd
```

## Packaging

PyInstaller helpers are available under [`tools/dev`](tools/dev).

Build a Windows bundle with:

```cmd
tools\dev\build_pyinstaller.cmd
```

The packaged application is written to:

```text
artifacts/pyinstaller/dist/grasp-desktop/
```

## Repository layout

```text
src/grasp/     Application code
tests/         Unit and integration-oriented tests
tools/dev/     Local packaging helpers
scripts/       Supporting scripts
```

## Notes

- GRASP currently targets `PySide6` only.
- The initial automatic pass is heuristic and local for speed.
- Manual Review actions can invoke OpenAI-based enrichment and live source lookup.
- User-provided API keys should be kept out of the repository and supplied through app settings or environment variables.
- Large local sample data and generated outputs are intentionally excluded from version control.
