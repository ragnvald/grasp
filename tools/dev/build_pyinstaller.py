from __future__ import annotations

import subprocess
import sys
from pathlib import Path


APP_NAME = "grasp-desktop"
GEOSTACK_PACKAGES = (
    "geopandas",
    "pandas",
    "pyarrow",
    "shapely",
    "fiona",
    "pyproj",
)


def main() -> int:
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

    for package in GEOSTACK_PACKAGES:
        cmd.extend(["--collect-all", package])

    for hidden_import in _qt_hidden_imports():
        cmd.extend(["--hidden-import", hidden_import])

    for excluded_module in _qt_excluded_modules():
        cmd.extend(["--exclude-module", excluded_module])

    cmd.append(str(entry))
    return subprocess.call(cmd, cwd=root)


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
    ]


if __name__ == "__main__":
    raise SystemExit(main())

