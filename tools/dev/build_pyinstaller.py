from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


APP_NAME = "grasp-desktop"
GEOSTACK_BINARY_PACKAGES = (
    "pyarrow",
    "shapely",
    "fiona",
    "pyproj",
)
GEOSTACK_DATA_PACKAGES = (
    "fiona",
    "pyproj",
)
GEOSTACK_METADATA_PACKAGES = (
    "geopandas",
    "pandas",
    "pyarrow",
    "shapely",
    "fiona",
    "pyproj",
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    src_dir = root / "src"
    entry = src_dir / "grasp" / "app.py"
    assets_dir = src_dir / "grasp" / "ui" / "assets"
    artifact_root = root / "artifacts" / "pyinstaller"
    dist_dir = artifact_root / "dist"
    build_dir = artifact_root / "build"
    spec_dir = artifact_root / "spec"

    for path in (artifact_root, dist_dir, build_dir, spec_dir):
        path.mkdir(parents=True, exist_ok=True)

    separator = ";" if sys.platform.startswith("win") else ":"
    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--windowed",
        "--onedir",
        "--name",
        APP_NAME,
        "--paths",
        str(src_dir),
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(build_dir),
        "--specpath",
        str(spec_dir),
    ]

    for asset in sorted(assets_dir.iterdir()):
        if asset.is_file():
            cmd.extend(["--add-data", f"{asset}{separator}grasp/ui/assets"])

    for package in GEOSTACK_BINARY_PACKAGES:
        cmd.extend(["--collect-binaries", package])

    for package in GEOSTACK_DATA_PACKAGES:
        cmd.extend(["--collect-data", package])

    for package in GEOSTACK_METADATA_PACKAGES:
        cmd.extend(["--copy-metadata", package])

    for hidden_import in _qt_hidden_imports():
        cmd.extend(["--hidden-import", hidden_import])

    for excluded_module in _qt_excluded_modules():
        cmd.extend(["--exclude-module", excluded_module])

    cmd.append(str(entry))
    exit_code = subprocess.call(cmd, cwd=root)
    if exit_code == 0 and args.portable_win11:
        _prepare_portable_layout(dist_dir / APP_NAME)
    return exit_code


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the GRASP desktop app with PyInstaller.")
    parser.add_argument(
        "--portable-win11",
        action="store_true",
        help="Prepare a portable Windows onedir layout with a sibling data_out folder.",
    )
    return parser.parse_args(argv)


def _prepare_portable_layout(app_dir: Path) -> None:
    (app_dir / "data_out").mkdir(parents=True, exist_ok=True)


def _qt_hidden_imports() -> list[str]:
    return [
        "PySide6.QtCore",
        "PySide6.QtGui",
        "PySide6.QtWidgets",
        "PySide6.QtWebChannel",
        "PySide6.QtWebEngineCore",
        "PySide6.QtWebEngineWidgets",
    ]


def _qt_excluded_modules() -> list[str]:
    return [
        "PyQt5",
        "PyQt5.QtCore",
        "PyQt5.QtGui",
        "PyQt5.QtWidgets",
        "PyQt5.QtWebChannel",
        "PyQt5.QtWebEngineWidgets",
        "PyQtWebEngine",
        "PyQtWebEngine.QtWebEngineCore",
        "PyQtWebEngine.QtWebEngineWidgets",
        "pyogrio",
        "pyogrio._io",
        "geopandas.tests",
        "geopandas.io.tests",
        "geopandas.tools.tests",
        "pandas.tests",
        "pyarrow.tests",
        "shapely.tests",
        "pytest",
        "_pytest",
        "py",
        "IPython",
        "matplotlib",
        "jinja2",
    ]


if __name__ == "__main__":
    raise SystemExit(main())
