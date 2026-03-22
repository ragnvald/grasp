from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import patch

import grasp.qt_compat as qt_compat
from grasp.runtime import configure_qt_runtime, consume_runtime_flags


class RuntimeTests(unittest.TestCase):
    def test_consume_runtime_flags_enables_webengine_disable_env(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GRASP_DISABLE_WEBENGINE", None)

            remaining = consume_runtime_flags(["--disable-webengine", "--example", "value"])

            self.assertEqual(remaining, ["--example", "value"])
            self.assertEqual(os.environ["GRASP_DISABLE_WEBENGINE"], "1")

    def test_configure_qt_runtime_adds_windows_safe_rendering_flags(self) -> None:
        env = {"QTWEBENGINE_CHROMIUM_FLAGS": "--existing-flag"}

        remaining = configure_qt_runtime(
            ["--disable-webengine", "--extra-arg"],
            environ=env,
            platform="win32",
        )

        self.assertEqual(remaining, ["--extra-arg"])
        self.assertEqual(env["GRASP_DISABLE_WEBENGINE"], "1")
        self.assertEqual(env["QT_OPENGL"], "software")
        self.assertEqual(env["QT_QUICK_BACKEND"], "software")
        self.assertEqual(env["QSG_RHI_PREFER_SOFTWARE_RENDERER"], "1")
        self.assertIn("--existing-flag", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--disable-gpu", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--disable-gpu-compositing", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--use-angle=swiftshader", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--disable-logging", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--log-level=3", env["QTWEBENGINE_CHROMIUM_FLAGS"])
        self.assertIn("--disable-features=VizDisplayCompositor", env["QTWEBENGINE_CHROMIUM_FLAGS"])

    def test_qt_compat_can_disable_webengine_via_environment(self) -> None:
        original = os.environ.get("GRASP_DISABLE_WEBENGINE")
        try:
            os.environ["GRASP_DISABLE_WEBENGINE"] = "1"
            reloaded = importlib.reload(qt_compat)

            self.assertTrue(reloaded.WEBENGINE_DISABLED_BY_ENV)
            self.assertFalse(reloaded.WEBENGINE_AVAILABLE)
            self.assertIn("--disable-webengine", reloaded.WEBENGINE_UNAVAILABLE_MESSAGE)
        finally:
            if original is None:
                os.environ.pop("GRASP_DISABLE_WEBENGINE", None)
            else:
                os.environ["GRASP_DISABLE_WEBENGINE"] = original
            importlib.reload(qt_compat)
