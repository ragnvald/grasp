# Developer Tools

This folder contains local development and packaging helpers.

## PyInstaller build

Windows launcher:

```cmd
tools\dev\build_pyinstaller.cmd
```

Direct Python entry point:

```powershell
.venv\Scripts\python.exe tools/dev/build_pyinstaller.py
```

Build output is written to:

```text
artifacts/pyinstaller/
  build/
  dist/grasp-desktop/
  spec/
```

The build runs in `--onedir` mode and collects the Qt and geospatial runtime stack used by the desktop app.

The project is packaged against `PySide6` only. Avoid installing `PyQt5` or `PyQtWebEngine` in the same build environment.
