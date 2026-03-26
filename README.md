# GRASP

GRASP stands for **Geo-data Retrieval, Analysis, Styling and Packaging**.

GRASP is a Windows-first desktop application for turning a folder of mixed GIS vector data into a reviewed local catalog with dataset metadata, grouping, source hints, map preview, styling, and export.

The project is authored by **Ragnvald Larsen**.

## Project status

GRASP is no longer just a scaffold. The current application supports an end-to-end local workflow:

1. Open a folder and create or reopen a local catalog.
2. Scan supported GIS vector files recursively.
3. Cache normalized dataset copies for repeatable processing.
4. Build local first-pass names, descriptions, themes, and grouping hints.
5. Optionally enrich understanding with OpenAI and live source search.
6. Review datasets in a grouped tree with batch actions.
7. Preview layers in an embedded map.
8. Export selected datasets as GeoPackage.

The repository also includes tests for import, review, map bridge behavior, styling-related assets, and export-facing logic.

## What the app does today

### Import

- Scans folders recursively for supported vector formats.
- Profiles geometry type, feature count, CRS, fields, and selected sample values.
- Stores a local project workspace under `data_out/`.
- Keeps raw source data read-only and works from cached derived files.

### Review

- Shows datasets in a grouped review tree.
- Supports checked-dataset batch actions such as:
  - select/clear by scope
  - apply suggested group
  - AI regroup
  - fill empty user fields from AI fields
  - make visible in maps / hide from maps
  - include in export / exclude from export
- Lets the user edit preferred names, descriptions, grouping, visibility, and export flags.
- Writes a persistent activity log for long-running work.

### Intelligence and source lookup

- Includes a local heuristic classifier for fast first-pass understanding.
- Can call OpenAI for richer dataset naming, descriptions, and grouping.
- Can search for likely source material and rank candidates.
- Falls back to local heuristics when remote AI or live search is unavailable.

### Map and styling

- Uses `QWebEngineView` with a Leaflet-based embedded map when WebEngine is available.
- Includes a local/offline fallback renderer path for degraded environments.
- Uses conservative visibility defaults to keep large projects responsive.
- Generates dataset styling heuristics and packages styling metadata into export output.
- Adds extra logging around map bridge and JavaScript/runtime issues to make renderer problems easier to diagnose.

### Export

- Exports selected datasets to:
  - GeoPackage
- GeoPackage export also writes:
  - metadata tables
  - layer style content
  - sidecar QGIS project files (`.qgs` and `.qgz`)

## Supported formats

Input:

- Shapefile (`.shp`)
- GeoPackage (`.gpkg`, including multiple layers)
- GeoJSON (`.geojson`)
- GeoJSON-like JSON (`.json`)
- GeoParquet / Parquet (`.parquet`)

Output:

- GeoPackage
- GeoPackage sidecar QGIS projects (`.qgs`, `.qgz`)

## Project workspace

GRASP keeps a local working area under `data_out/`, but the location depends on how the app is run.

- In local source/dev runs, `data_out/` is created inside the selected source folder.
- In the compiled portable Windows build, `data_out/` is created beside `grasp-desktop.exe`.
- In the compiled portable Windows build, each opened source folder gets its own workspace subfolder inside that shared portable `data_out/` area.

Typical layout:

```text
data_out/
  <project-id>/
    catalog.sqlite
    cache/
      datasets/
    exports/
    logs/
    temp/
    log.txt
```

This workspace contains the local catalog, cached datasets, exports, and logs for a specific source folder.

## Technology

- Python 3.11+
- `PySide6` for the desktop UI
- `QWebEngineView` + `QWebChannel` for the embedded map surface
- `geopandas`, `fiona`, `shapely`, `pyarrow`, `pandas` for ingest and export
- SQLite for local catalog state
- PyInstaller for Windows packaging

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

Run tests:

```powershell
pytest
```

## Settings

The Settings tab currently covers:

- OpenAI API key
- OpenAI model and endpoint
- OpenAI timeout and failure thresholds
- what dataset context is included in AI classification prompts
- live search timeout and candidate count

If no OpenAI key is configured, GRASP still works with local heuristics.

## Packaging

PyInstaller helpers are available under [`tools/dev`](tools/dev).

Build a Windows bundle with:

```cmd
tools\dev\build_pyinstaller.cmd
```

Build the portable Windows 11 distribution layout with:

```cmd
tools\dev\portable-win11.cmd
```

or:

```powershell
.venv\Scripts\python.exe tools/dev/build_pyinstaller.py
```

or:

```powershell
.venv\Scripts\python.exe tools/dev/build_pyinstaller.py --portable-win11
```

The packaged application is written to:

```text
artifacts/pyinstaller/dist/grasp-desktop/
```

The build currently targets a standalone Windows `--onedir` bundle.

In the portable Windows layout, the distribution folder is intended to be moved as a unit and the application creates its runtime `data_out/` beside the executable.

## Repository layout

```text
src/grasp/     Application code
tests/         Unit and integration-oriented tests
tools/dev/     Local packaging helpers
scripts/       Supporting scripts
```

## Notes and limitations

- GRASP currently targets `PySide6` only.
- The desktop app is Windows-first, and the packaging workflow is oriented around standalone Windows builds.
- The compiled distribution is portable as a folder-based `--onedir` build, not a single-file executable.
- The fast/local first pass is heuristic by design and should be treated as a draft interpretation.
- Live source search depends on network availability and may degrade to placeholders or local fallbacks.
- OpenAI-powered enrichment is optional and depends on user-provided credentials.
- Large local sample data and generated outputs are intentionally excluded from version control.
