from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
import unittest


REPO_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _run_script_import(script_rel_path: str, *, install_matplotlib_stubs: bool = False) -> subprocess.CompletedProcess[str]:
    script_path = os.path.join(REPO_DIR, script_rel_path)
    stub_flag = "1" if install_matplotlib_stubs else "0"
    code = textwrap.dedent(
        """
        import runpy
        import sys
        import types

        script_path = sys.argv[1]
        install_stubs = sys.argv[2] == "1"

        if install_stubs:
            matplotlib = types.ModuleType("matplotlib")
            matplotlib.__path__ = []
            figure_mod = types.ModuleType("matplotlib.figure")
            patches_mod = types.ModuleType("matplotlib.patches")
            ticker_mod = types.ModuleType("matplotlib.ticker")
            transforms_mod = types.ModuleType("matplotlib.transforms")
            backends_mod = types.ModuleType("matplotlib.backends")
            backends_mod.__path__ = []
            backend_tkagg_mod = types.ModuleType("matplotlib.backends.backend_tkagg")

            class Figure:
                pass

            class Rectangle:
                pass

            class FuncFormatter:
                def __init__(self, func=None):
                    self.func = func

            class FigureCanvasTkAgg:
                def __init__(self, *args, **kwargs):
                    self.args = args
                    self.kwargs = kwargs

            def blended_transform_factory(*args, **kwargs):
                return None

            figure_mod.Figure = Figure
            patches_mod.Rectangle = Rectangle
            ticker_mod.FuncFormatter = FuncFormatter
            transforms_mod.blended_transform_factory = blended_transform_factory
            backend_tkagg_mod.FigureCanvasTkAgg = FigureCanvasTkAgg

            sys.modules["matplotlib"] = matplotlib
            sys.modules["matplotlib.figure"] = figure_mod
            sys.modules["matplotlib.patches"] = patches_mod
            sys.modules["matplotlib.ticker"] = ticker_mod
            sys.modules["matplotlib.transforms"] = transforms_mod
            sys.modules["matplotlib.backends"] = backends_mod
            sys.modules["matplotlib.backends.backend_tkagg"] = backend_tkagg_mod

        runpy.run_path(script_path, run_name="__entrypoint_test__")
        print("ENTRYPOINT_OK")
        """
    )
    with tempfile.TemporaryDirectory() as td:
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env["MPLCONFIGDIR"] = os.path.join(td, ".mplconfig")
        return subprocess.run(
            [sys.executable, "-c", code, script_path, stub_flag],
            cwd=td,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )


class EntrypointTests(unittest.TestCase):
    def test_ui_hub_script_bootstraps_repo_root(self) -> None:
        proc = _run_script_import("ui/pt_hub.py", install_matplotlib_stubs=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)
        self.assertIn("ENTRYPOINT_OK", proc.stdout)

    def test_runner_script_bootstraps_repo_root(self) -> None:
        proc = _run_script_import("runtime/pt_runner.py")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)
        self.assertIn("ENTRYPOINT_OK", proc.stdout)

    def test_markets_script_bootstraps_repo_root(self) -> None:
        proc = _run_script_import("runtime/pt_markets.py")
        self.assertEqual(proc.returncode, 0, msg=proc.stderr or proc.stdout)
        self.assertIn("ENTRYPOINT_OK", proc.stdout)

    def test_launchd_template_uses_project_venv_and_module_invocation(self) -> None:
        path = os.path.join(REPO_DIR, "com.powertrader.runner.plist.template")
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        self.assertIn("/Users/joeydelestre/PowerTrader_AI/venv/bin/python3", text)
        self.assertIn("<string>-m</string>", text)
        self.assertIn("<string>runtime.pt_runner</string>", text)
        self.assertIn("<key>PYTHONPATH</key>", text)
        self.assertIn("<key>MPLCONFIGDIR</key>", text)


if __name__ == "__main__":
    unittest.main()
