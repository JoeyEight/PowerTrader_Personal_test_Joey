from __future__ import annotations

import math
import unittest

from app.status_hydration import (
    market_status_has_account_snapshot,
    needs_market_snapshot_refresh,
    payload_age_seconds,
)


class TestStatusHydration(unittest.TestCase):
    def test_payload_age_seconds_uses_ts_field(self) -> None:
        age = payload_age_seconds({"ts": 100.0}, now_ts=125.5)
        self.assertAlmostEqual(age, 25.5)

    def test_payload_age_seconds_returns_inf_when_missing_timestamp(self) -> None:
        self.assertTrue(math.isinf(payload_age_seconds({}, now_ts=100.0)))

    def test_market_status_has_account_snapshot_accepts_equity_fallback(self) -> None:
        self.assertTrue(market_status_has_account_snapshot({"equity": "99999.49"}))
        self.assertTrue(market_status_has_account_snapshot({"raw_positions": [{"symbol": "AMZN"}]}))
        self.assertFalse(market_status_has_account_snapshot({"buying_power": "Pending account link"}))

    def test_needs_market_snapshot_refresh_false_for_fresh_ready_snapshot(self) -> None:
        now_ts = 1_000.0
        status = {
            "ts": now_ts - 5.0,
            "state": "READY",
            "buying_power": "199949.49",
            "equity": "99999.49",
        }
        loop = {"heartbeat_ts": now_ts - 4.0}
        self.assertFalse(
            needs_market_snapshot_refresh(
                status,
                loop,
                "stocks",
                now_ts=now_ts,
                stale_after_s=30.0,
            )
        )

    def test_needs_market_snapshot_refresh_true_for_placeholder_snapshot(self) -> None:
        now_ts = 1_000.0
        status = {
            "ts": now_ts - 3.0,
            "state": "NOT CONFIGURED",
            "buying_power": "Pending account link",
        }
        loop = {"heartbeat_ts": now_ts - 2.0}
        self.assertTrue(
            needs_market_snapshot_refresh(
                status,
                loop,
                "stocks",
                now_ts=now_ts,
                stale_after_s=30.0,
            )
        )

    def test_needs_market_snapshot_refresh_true_for_stale_loop_heartbeat(self) -> None:
        now_ts = 1_000.0
        status = {
            "ts": now_ts - 6.0,
            "state": "READY",
            "buying_power": "199949.49",
        }
        loop = {"heartbeat_ts": now_ts - 65.0}
        self.assertTrue(
            needs_market_snapshot_refresh(
                status,
                loop,
                "stocks",
                now_ts=now_ts,
                stale_after_s=30.0,
            )
        )


if __name__ == "__main__":
    unittest.main()
