from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import shutil
import sys


SUPPORTED_SUFFIXES = {".shp", ".gpkg", ".geojson", ".json", ".parquet"}
PRIMARY_WORKSPACE_DIRNAME = "data_out"
WORKSPACE_DIRNAMES = {PRIMARY_WORKSPACE_DIRNAME}


@dataclass(slots=True)
class ProjectWorkspace:
    root_path: Path
    workspace_path: Path
    db_path: Path
    cache_dir: Path
    exports_dir: Path
    logs_dir: Path
    temp_dir: Path

    def dataset_cache_path(self, dataset_id: str) -> Path:
        return self.cache_dir / f"{dataset_id}.parquet"

    def temp_path(self, name: str) -> Path:
        return self.temp_dir / name

    def activity_log_path(self) -> Path:
        return self.workspace_path / "log.txt"

    def log_path(self, name: str = "app.log") -> Path:
        return self.logs_dir / name

    def resolve_cache_path(self, dataset_id: str, cache_path: str = "") -> Path:
        if cache_path:
            path = Path(cache_path)
            if path.is_absolute():
                return path
            return (self.root_path / path).resolve()
        return self.dataset_cache_path(dataset_id)

    def clear_temp_dir(self) -> None:
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        for path in self.temp_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass

    def cleanup_orphaned_cache_files(self, active_dataset_ids: set[str]) -> int:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        removed = 0
        for path in self.cache_dir.glob("*.parquet"):
            if path.stem in active_dataset_ids:
                continue
            try:
                path.unlink()
                removed += 1
            except FileNotFoundError:
                continue
        return removed


def ensure_workspace(root_path: str | Path) -> ProjectWorkspace:
    root = Path(root_path).expanduser().resolve()
    workspace = _prepare_workspace_root(root)
    cache_dir = workspace / "cache" / "datasets"
    exports_dir = workspace / "exports"
    logs_dir = workspace / "logs"
    temp_dir = workspace / "temp"
    for path in (workspace, cache_dir, exports_dir, logs_dir, temp_dir):
        path.mkdir(parents=True, exist_ok=True)
    return ProjectWorkspace(
        root_path=root,
        workspace_path=workspace,
        db_path=workspace / "catalog.sqlite",
        cache_dir=cache_dir,
        exports_dir=exports_dir,
        logs_dir=logs_dir,
        temp_dir=temp_dir,
    )


def catalog_exists(root_path: str | Path) -> bool:
    root = Path(root_path).expanduser().resolve()
    return (_prepare_workspace_root(root) / "catalog.sqlite").exists()


def iter_supported_files(root_path: str | Path) -> list[Path]:
    root = Path(root_path).expanduser().resolve()
    excluded_roots = _excluded_workspace_roots(root)
    matches: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(_is_relative_to(path.resolve(), excluded_root) for excluded_root in excluded_roots):
            continue
        if path.suffix.lower() in SUPPORTED_SUFFIXES:
            matches.append(path)
    return sorted(matches)


def _prepare_workspace_root(root: Path) -> Path:
    if _is_frozen_runtime():
        return _portable_workspace_base() / _portable_project_dirname(root)
    return root / PRIMARY_WORKSPACE_DIRNAME


def _excluded_workspace_roots(root: Path) -> list[Path]:
    workspace = _prepare_workspace_root(root).resolve()
    if _is_relative_to(workspace, root):
        return [workspace]
    return []


def _portable_workspace_base() -> Path:
    return Path(sys.executable).resolve().parent / PRIMARY_WORKSPACE_DIRNAME


def _portable_project_dirname(root: Path) -> str:
    readable_name = re.sub(r"[^A-Za-z0-9._-]+", "-", root.name or "project").strip("-._")
    if not readable_name:
        readable_name = "project"
    digest = hashlib.sha1(root.as_posix().encode("utf-8")).hexdigest()[:12]
    return f"{readable_name}-{digest}"


def _is_frozen_runtime() -> bool:
    return bool(getattr(sys, "frozen", False))


def _is_relative_to(path: Path, other: Path) -> bool:
    try:
        path.relative_to(other)
        return True
    except ValueError:
        return False


def make_dataset_id(root_path: str | Path, source_path: str | Path, layer_name: str = "") -> str:
    root = Path(root_path).expanduser().resolve()
    source = Path(source_path).expanduser().resolve()
    try:
        relative = source.relative_to(root).as_posix()
    except ValueError:
        relative = source.as_posix()
    payload = f"{relative}|{layer_name}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:16]


def sanitize_group_id(value: str) -> str:
    if not value:
        return "ungrouped"
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned or "ungrouped"


def display_group_name(group_id: str) -> str:
    if group_id == "ungrouped":
        return "Ungrouped"
    return group_id.replace("-", " ").title()


def sanitize_layer_name(value: str, existing: set[str] | None = None) -> str:
    existing = existing or set()
    candidate = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_")
    candidate = candidate[:48] or "layer"
    if candidate not in existing:
        return candidate
    index = 2
    while True:
        suffix = f"_{index}"
        trimmed = candidate[: max(1, 48 - len(suffix))]
        proposed = f"{trimmed}{suffix}"
        if proposed not in existing:
            return proposed
        index += 1

