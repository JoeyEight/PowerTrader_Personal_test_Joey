from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import types
import unittest


def _install_matplotlib_stubs() -> None:
    if "matplotlib.figure" in sys.modules:
        return

    matplotlib = types.ModuleType("matplotlib")
    matplotlib.__path__ = []
    figure_mod = types.ModuleType("matplotlib.figure")
    patches_mod = types.ModuleType("matplotlib.patches")
    ticker_mod = types.ModuleType("matplotlib.ticker")
    transforms_mod = types.ModuleType("matplotlib.transforms")
    backends_mod = types.ModuleType("matplotlib.backends")
    backends_mod.__path__ = []
    backend_tkagg_mod = types.ModuleType("matplotlib.backends.backend_tkagg")

    class Figure:  # pragma: no cover - import shim only
        pass

    class Rectangle:  # pragma: no cover - import shim only
        pass

    class FuncFormatter:  # pragma: no cover - import shim only
        def __init__(self, func=None) -> None:
            self.func = func

    class FigureCanvasTkAgg:  # pragma: no cover - import shim only
        def __init__(self, *args, **kwargs) -> None:
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


def _load_pt_hub_module():
    _install_matplotlib_stubs()
    return importlib.import_module("ui.pt_hub")


class _Axis:
    class _YAxis:
        def __init__(self) -> None:
            self.formatter = None

        def set_major_formatter(self, formatter) -> None:
            self.formatter = formatter

    def __init__(self) -> None:
        self.lines = []
        self.patches = []
        self.collections = []
        self.texts = []
        self.annotations = []
        self.title = ""
        self.xticks = []
        self.xticklabels = []
        self.xlim = None
        self.yaxis = self._YAxis()

    def cla(self) -> None:
        self.lines.clear()
        self.patches.clear()
        self.collections.clear()
        self.texts.clear()
        self.annotations.clear()

    def plot(self, xs, ys, linewidth=1.5):
        self.lines.append((list(xs), list(ys), float(linewidth)))

    def scatter(self, xs, ys, **kwargs) -> None:
        self.collections.append((list(xs), list(ys), dict(kwargs)))

    def annotate(self, label, xy, **kwargs) -> None:
        self.annotations.append((str(label), tuple(xy), dict(kwargs)))

    def minorticks_off(self) -> None:
        pass

    def set_xticks(self, ticks) -> None:
        self.xticks = list(ticks)

    def set_xticklabels(self, labels) -> None:
        self.xticklabels = list(labels)

    def tick_params(self, **kwargs) -> None:
        pass

    def set_xlim(self, left, right) -> None:
        self.xlim = (float(left), float(right))

    def set_title(self, title, color=None) -> None:
        self.title = str(title)

    def text(self, *args, **kwargs) -> None:
        self.texts.append((args, kwargs))


class _Canvas:
    def __init__(self) -> None:
        self.draw_idle_calls = 0

    def draw_idle(self) -> None:
        self.draw_idle_calls += 1


class _Label:
    def __init__(self) -> None:
        self.kwargs = {}

    def config(self, **kwargs) -> None:
        self.kwargs.update(kwargs)


class AccountValueChartTests(unittest.TestCase):
    def test_refresh_keeps_trade_annotations_for_all_coins_chart(self) -> None:
        pt_hub = _load_pt_hub_module()
        chart_cls = pt_hub.AccountValueChart

        with tempfile.TemporaryDirectory() as td:
            history_path = os.path.join(td, "account_value_history.jsonl")
            trade_history_path = os.path.join(td, "trade_history.jsonl")
            with open(history_path, "w", encoding="utf-8") as f:
                for ts, value in ((100, 1000.0), (200, 1005.0), (300, 1010.0)):
                    f.write(json.dumps({"ts": ts, "total_account_value": value}) + "\n")
            with open(trade_history_path, "w", encoding="utf-8") as f:
                f.write(json.dumps({"ts": 200, "side": "buy", "symbol": "BTC-USD"}) + "\n")

            chart = types.SimpleNamespace(
                history_path=history_path,
                trade_history_path=trade_history_path,
                max_points=250,
                _last_mtime=None,
                ax=_Axis(),
                canvas=_Canvas(),
                last_update_label=_Label(),
                _apply_dark_chart_style=lambda: None,
            )

            chart_cls.refresh(chart)

            self.assertEqual(len(chart.ax.collections), 1)
            self.assertEqual(len(chart.ax.annotations), 1)
            self.assertEqual(chart.ax.annotations[0][0], "BTC BUY")
            self.assertGreater(chart.canvas.draw_idle_calls, 0)


if __name__ == "__main__":
    unittest.main()
