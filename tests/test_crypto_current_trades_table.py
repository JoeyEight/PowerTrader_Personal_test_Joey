from __future__ import annotations

import sys
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


_install_matplotlib_stubs()

from ui.pt_hub import PowerTraderHub


class _FakeCanvas:
    def __init__(self) -> None:
        self._next_id = 1
        self.items = {}
        self.itemconfigure_calls = []
        self.delete_calls = []
        self.deleted_all = False
        self.config = {}

    def winfo_width(self) -> int:
        return 960

    def winfo_height(self) -> int:
        return 240

    def configure(self, **kwargs) -> None:
        self.config.update(kwargs)

    def _create_item(self, kind: str, coords, **kwargs) -> int:
        item_id = self._next_id
        self._next_id += 1
        self.items[item_id] = {"kind": kind, "coords": tuple(coords), "config": dict(kwargs)}
        return item_id

    def create_rectangle(self, *coords, **kwargs) -> int:
        return self._create_item("rectangle", coords, **kwargs)

    def create_text(self, *coords, **kwargs) -> int:
        return self._create_item("text", coords, **kwargs)

    def create_line(self, *coords, **kwargs) -> int:
        return self._create_item("line", coords, **kwargs)

    def coords(self, item_id: int, *coords) -> None:
        self.items[item_id]["coords"] = tuple(coords)

    def itemconfigure(self, item_id: int, **kwargs) -> None:
        self.itemconfigure_calls.append((int(item_id), dict(kwargs)))
        self.items[item_id]["config"].update(kwargs)

    def delete(self, item_id) -> None:
        self.delete_calls.append(item_id)
        if item_id == "all":
            self.deleted_all = True
            self.items.clear()
            return
        self.items.pop(int(item_id), None)


class CryptoCurrentTradesTableTests(unittest.TestCase):
    def _make_hub(self) -> PowerTraderHub:
        hub = PowerTraderHub.__new__(PowerTraderHub)
        hub.trades_cols = ("coin", "qty", "value")
        hub.trades_numeric_cols = {"qty", "value"}
        hub.trades_center_cols = set()
        hub.trades_header_labels = {"coin": "Coin", "qty": "Qty", "value": "Value"}
        hub._trades_base_widths = {"coin": 90, "qty": 90, "value": 120}
        hub._trades_table_rows = []
        hub._trades_table_sig = None
        hub._trades_table_render_state = {}
        hub._trades_header_height = 28
        hub._trades_row_height = 28
        return hub

    def test_set_trades_table_rows_skips_redraw_when_display_signature_is_unchanged(self) -> None:
        hub = self._make_hub()
        draw_calls = []

        def _draw(_self) -> None:
            draw_calls.append(tuple((row.get("coin"), row.get("qty"), row.get("value")) for row in _self._trades_table_rows))

        hub._draw_trades_table = types.MethodType(_draw, hub)

        first = [{"coin": "BTC", "qty": "1", "value": "$100.00"}]
        second = [{"coin": "BTC", "qty": "1", "value": "$100.00"}]
        changed = [{"coin": "BTC", "qty": "2", "value": "$100.00"}]

        self.assertTrue(PowerTraderHub._set_trades_table_rows(hub, first))
        self.assertFalse(PowerTraderHub._set_trades_table_rows(hub, second))
        self.assertTrue(PowerTraderHub._set_trades_table_rows(hub, changed))
        self.assertEqual(len(draw_calls), 2)

    def test_draw_trades_table_updates_only_changed_cell_without_full_canvas_clear(self) -> None:
        hub = self._make_hub()
        canvas = _FakeCanvas()
        hub.trades_canvas = canvas
        hub._trades_table_rows = [{"coin": "BTC", "qty": "1", "value": "$100.00"}]

        PowerTraderHub._draw_trades_table(hub)

        initial_item_count = len(canvas.items)
        canvas.itemconfigure_calls.clear()
        canvas.delete_calls.clear()
        hub._trades_table_rows = [{"coin": "BTC", "qty": "2", "value": "$100.00"}]

        PowerTraderHub._draw_trades_table(hub)

        text_updates = [kwargs for _, kwargs in canvas.itemconfigure_calls if "text" in kwargs]
        self.assertFalse(canvas.deleted_all)
        self.assertEqual(len(canvas.items), initial_item_count)
        self.assertEqual(text_updates, [{"text": "2"}])
        self.assertNotIn("all", canvas.delete_calls)


if __name__ == "__main__":
    unittest.main()
