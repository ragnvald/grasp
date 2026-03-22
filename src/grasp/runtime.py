from __future__ import annotations

import os
import sys


def consume_runtime_flags(argv: list[str], *, environ: dict[str, str] | None = None) -> list[str]:
    env = os.environ if environ is None else environ
    remaining: list[str] = []
    for arg in argv:
        if arg == "--disable-webengine":
            env["GRASP_DISABLE_WEBENGINE"] = "1"
            continue
        remaining.append(arg)
    return remaining


def append_env_flag(name: str, flag: str, *, environ: dict[str, str] | None = None) -> None:
    env = os.environ if environ is None else environ
    current = env.get(name, "").strip()
    if not current:
        env[name] = flag
        return
    flags = current.split()
    if flag not in flags:
        env[name] = f"{current} {flag}".strip()


def configure_qt_runtime(
    argv: list[str],
    *,
    environ: dict[str, str] | None = None,
    platform: str | None = None,
) -> list[str]:
    env = os.environ if environ is None else environ
    runtime_platform = sys.platform if platform is None else platform
    cleaned_argv = consume_runtime_flags(argv, environ=env)
    if str(runtime_platform).startswith("win"):
        env.setdefault("QT_OPENGL", "software")
        env.setdefault("QT_QUICK_BACKEND", "software")
        env.setdefault("QSG_RHI_PREFER_SOFTWARE_RENDERER", "1")
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu", environ=env)
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-gpu-compositing", environ=env)
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--use-angle=swiftshader", environ=env)
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-logging", environ=env)
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--log-level=3", environ=env)
        append_env_flag("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-features=VizDisplayCompositor", environ=env)
    return cleaned_argv
