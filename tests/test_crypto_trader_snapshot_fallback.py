from __future__ import annotations

import base64
import importlib
import json
import os
import tempfile
import time
import unittest


def _load_pt_trader_module():
    os.environ.setdefault("POWERTRADER_RH_API_KEY", "test-key")
    os.environ.setdefault("POWERTRADER_RH_PRIVATE_B64", base64.b64encode(b"0" * 32).decode("ascii"))
    return importlib.import_module("engines.pt_trader")


class CryptoTraderSnapshotFallbackTests(unittest.TestCase):
    def test_bootstrap_last_good_snapshot_from_disk_seeds_account_and_holdings(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot._pnl_ledger = {"open_positions": {}, "pending_orders": {}}
        bot._last_good_account_snapshot = {
            "total_account_value": None,
            "buying_power": None,
            "holdings_sell_value": None,
            "holdings_buy_value": None,
            "percent_in_trade": None,
        }
        bot._last_good_holdings_results = []
        bot._last_good_holdings_ts = 0.0
        bot._last_good_positions_snapshot = {}

        with tempfile.TemporaryDirectory() as td:
            snapshot_path = os.path.join(td, "trader_data.json")
            with open(snapshot_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timestamp": 1234567890.0,
                        "account": {
                            "total_account_value": 102.97,
                            "buying_power": 94.93,
                            "holdings_sell_value": 8.04,
                            "holdings_buy_value": 8.17,
                            "percent_in_trade": 7.81,
                        },
                        "positions": {
                            "BTC": {"quantity": 0.01, "avg_cost_basis": 70000.0, "dca_triggered_stages": 1},
                            "SOL": {"quantity": 0.5, "avg_cost_basis": 80.0, "dca_triggered_stages": 0},
                            "DOGE": {"quantity": 0.0, "avg_cost_basis": 0.0, "dca_triggered_stages": 0},
                        },
                    },
                    f,
                )

            orig_path = pt_trader.TRADER_DETAIL_PATH
            self.addCleanup(setattr, pt_trader, "TRADER_DETAIL_PATH", orig_path)
            pt_trader.TRADER_DETAIL_PATH = snapshot_path
            pt_trader.CryptoAPITrading._bootstrap_last_good_snapshot_from_disk(bot)

        self.assertEqual(bot._last_good_account_snapshot["buying_power"], 94.93)
        self.assertEqual(bot._last_good_account_snapshot["total_account_value"], 102.97)
        self.assertEqual(
            sorted(row["asset_code"] for row in bot._last_good_holdings_results),
            ["BTC", "SOL"],
        )
        self.assertEqual(sorted(bot._last_good_positions_snapshot.keys()), ["BTC", "SOL"])
        self.assertEqual(bot._last_good_holdings_ts, 1234567890.0)
        self.assertEqual(bot.cost_basis["BTC"], 70000.0)
        self.assertEqual(bot.cost_basis["SOL"], 80.0)

    def test_resolve_holdings_results_reuses_cached_rows_when_api_returns_empty(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot._pnl_ledger = {"open_positions": {"BTC": {"qty": 0.01, "usd_cost": 1.0}}, "pending_orders": {}}
        bot._last_good_holdings_results = [{"asset_code": "BTC", "total_quantity": 0.01}]
        bot._last_good_holdings_ts = time.time()
        bot._last_good_positions_snapshot = {}
        bot._status_note = ""

        rows, used_cached = pt_trader.CryptoAPITrading._resolve_holdings_results(
            bot,
            {"results": []},
            recent_trade=False,
        )

        self.assertTrue(used_cached)
        self.assertEqual(rows, [{"asset_code": "BTC", "total_quantity": 0.01}])
        self.assertIn("cached crypto positions", bot._status_note.lower())

    def test_resolve_holdings_results_does_not_reuse_cache_after_trade(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot._pnl_ledger = {"open_positions": {"BTC": {"qty": 0.01, "usd_cost": 1.0}}, "pending_orders": {}}
        bot._last_good_holdings_results = [{"asset_code": "BTC", "total_quantity": 0.01}]
        bot._last_good_holdings_ts = time.time()
        bot._last_good_positions_snapshot = {}
        bot._status_note = ""

        rows, used_cached = pt_trader.CryptoAPITrading._resolve_holdings_results(
            bot,
            {"results": []},
            recent_trade=True,
        )

        self.assertFalse(used_cached)
        self.assertEqual(rows, [])

    def test_seed_cost_basis_from_snapshot_fills_missing_symbols(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot._pnl_ledger = {"open_positions": {}, "pending_orders": {}}
        bot.cost_basis = {"BTC": 70000.0}
        bot._last_good_positions_snapshot = {
            "AAVE": {"avg_cost_basis": 111.92, "quantity": 0.0092},
            "SOL": {"avg_cost_basis": 86.66, "quantity": 0.01188},
            "BTC": {"avg_cost_basis": 70432.88, "quantity": 0.00001},
        }

        pt_trader.CryptoAPITrading._seed_cost_basis_from_fallbacks(bot)

        self.assertEqual(bot.cost_basis["BTC"], 70000.0)
        self.assertEqual(bot.cost_basis["AAVE"], 111.92)
        self.assertEqual(bot.cost_basis["SOL"], 86.66)

    def test_fallback_avg_cost_basis_uses_ledger_when_snapshot_missing(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot.cost_basis = {}
        bot._last_good_positions_snapshot = {}
        bot._pnl_ledger = {
            "open_positions": {"DOGE": {"qty": 8.58, "usd_cost": 0.8299999999999983}},
            "pending_orders": {},
        }

        avg = pt_trader.CryptoAPITrading._fallback_avg_cost_basis(bot, "DOGE", quantity=8.58)

        self.assertAlmostEqual(avg, 0.8299999999999983 / 8.58)

    def test_refresh_missing_cost_basis_recomputes_from_orders(self) -> None:
        pt_trader = _load_pt_trader_module()
        bot = object.__new__(pt_trader.CryptoAPITrading)
        bot.cost_basis = {}
        bot._last_good_positions_snapshot = {}
        bot._pnl_ledger = {"open_positions": {}, "pending_orders": {}}
        bot.get_orders = lambda symbol: {
            "results": [
                {
                    "side": "buy",
                    "state": "filled",
                    "created_at": "2026-03-10T12:00:00Z",
                    "executions": [
                        {"quantity": "0.003428", "effective_price": "5159.83343162"},
                        {"quantity": "0.001387", "effective_price": "5260.94800378"},
                    ],
                }
            ]
        }

        pt_trader.CryptoAPITrading._refresh_missing_cost_basis(
            bot,
            [{"asset_code": "PAXG", "total_quantity": 0.000809}],
        )

        self.assertGreater(bot.cost_basis["PAXG"], 0.0)


if __name__ == "__main__":
    unittest.main()
