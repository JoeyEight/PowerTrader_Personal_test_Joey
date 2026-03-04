from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List

from path_utils import resolve_runtime_paths

BASE_DIR, _SETTINGS_PATH, HUB_DATA_DIR, _BOOT_SETTINGS = resolve_runtime_paths(__file__, "stock_thinker")

DEFAULT_STOCK_UNIVERSE = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "TSLA", "SPY", "QQQ"]


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _request_json(url: str, headers: Dict[str, str], timeout: float = 10.0) -> Any:
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _score_bars(symbol: str, bars: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = []
    for row in bars or []:
        if not isinstance(row, dict):
            continue
        close_val = _float(row.get("c", row.get("close", 0.0)), 0.0)
        if close_val > 0:
            closes.append(close_val)
    if len(closes) < 8:
        return {
            "symbol": symbol,
            "score": -9999.0,
            "side": "watch",
            "last": closes[-1] if closes else 0.0,
            "change_6h_pct": 0.0,
            "change_24h_pct": 0.0,
            "volatility_pct": 0.0,
            "confidence": "LOW",
            "reason": "Not enough bars",
        }

    last_px = closes[-1]
    px_6 = closes[max(0, len(closes) - 7)]
    px_24 = closes[max(0, len(closes) - min(24, len(closes)))]
    change_6 = ((last_px - px_6) / px_6) * 100.0 if px_6 > 0 else 0.0
    change_24 = ((last_px - px_24) / px_24) * 100.0 if px_24 > 0 else 0.0

    step_moves = []
    for idx in range(1, len(closes)):
        prev_px = closes[idx - 1]
        cur_px = closes[idx]
        if prev_px > 0:
            step_moves.append(abs(((cur_px - prev_px) / prev_px) * 100.0))
    volatility = (sum(step_moves[-12:]) / max(1, len(step_moves[-12:]))) if step_moves else 0.0

    score = (change_6 * 0.65) + (change_24 * 0.25) + (volatility * 0.10)
    side = "long" if score > 0 else "watch"
    abs_score = abs(score)
    if abs_score >= 4.0:
        confidence = "HIGH"
    elif abs_score >= 1.75:
        confidence = "MED"
    else:
        confidence = "LOW"

    reason = f"6h {change_6:+.2f}% | 24h {change_24:+.2f}% | vol {volatility:.2f}%"
    return {
        "symbol": symbol,
        "score": round(score, 4),
        "side": side,
        "last": round(last_px, 6),
        "change_6h_pct": round(change_6, 4),
        "change_24h_pct": round(change_24, 4),
        "volatility_pct": round(volatility, 4),
        "confidence": confidence,
        "reason": reason,
    }


def run_scan(settings: Dict[str, Any], hub_dir: str) -> Dict[str, Any]:
    api_key = str(settings.get("alpaca_api_key_id", "") or "").strip()
    secret = str(settings.get("alpaca_secret_key", "") or "").strip()
    base_url = str(settings.get("alpaca_data_url", settings.get("alpaca_base_url", "https://data.alpaca.markets")) or "").strip().rstrip("/")

    if not api_key or not secret:
        return {
            "state": "NOT CONFIGURED",
            "ai_state": "Credentials missing",
            "msg": "Add Alpaca keys in Settings",
            "universe": list(DEFAULT_STOCK_UNIVERSE),
            "leaders": [],
            "updated_at": int(time.time()),
        }

    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret,
    }

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=10)
    start_iso = start_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    end_iso = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    scored: List[Dict[str, Any]] = []
    last_exc: Exception | None = None

    # Try IEX first (paper-friendly), then SIP fallback if account supports it.
    for feed in ("iex", "sip"):
        try:
            params = {
                "symbols": ",".join(DEFAULT_STOCK_UNIVERSE),
                "timeframe": "1Hour",
                "limit": "96",
                "adjustment": "raw",
                "feed": feed,
                "start": start_iso,
                "end": end_iso,
            }
            url = f"{base_url}/v2/stocks/bars?{urllib.parse.urlencode(params)}"
            payload = _request_json(url, headers=headers, timeout=12.0)
            bars_by_symbol = payload.get("bars", {}) or {}
            if not isinstance(bars_by_symbol, dict):
                bars_by_symbol = {}
            scored = []
            for symbol in DEFAULT_STOCK_UNIVERSE:
                scored.append(_score_bars(symbol, list(bars_by_symbol.get(symbol, []) or [])))
            if any(float(row.get("score", -9999.0)) > -9999.0 for row in scored):
                break
        except Exception as exc:
            last_exc = exc
            scored = []

    if not scored:
        if isinstance(last_exc, urllib.error.HTTPError):
            return {
                "state": "ERROR",
                "ai_state": "HTTP error",
                "msg": f"HTTP {last_exc.code}: {last_exc.reason}",
                "universe": list(DEFAULT_STOCK_UNIVERSE),
                "leaders": [],
                "updated_at": int(time.time()),
            }
        if isinstance(last_exc, urllib.error.URLError):
            return {
                "state": "ERROR",
                "ai_state": "Network error",
                "msg": f"Network error: {last_exc.reason}",
                "universe": list(DEFAULT_STOCK_UNIVERSE),
                "leaders": [],
                "updated_at": int(time.time()),
            }
        return {
            "state": "ERROR",
            "ai_state": "Scan failed",
            "msg": (f"{type(last_exc).__name__}: {last_exc}" if last_exc else "No bar data returned"),
            "universe": list(DEFAULT_STOCK_UNIVERSE),
            "leaders": [],
            "updated_at": int(time.time()),
        }

    scored.sort(key=lambda row: float(row.get("score", -9999.0)), reverse=True)
    leaders = [row for row in scored if str(row.get("side", "")).lower() == "long"][:5]
    top_pick = leaders[0] if leaders else (scored[0] if scored else None)
    msg = "No viable long candidates"
    if top_pick:
        msg = f"Top pick {top_pick['symbol']} | {top_pick['reason']}"
    return {
        "state": "READY",
        "ai_state": "Scan ready",
        "msg": msg,
        "universe": list(DEFAULT_STOCK_UNIVERSE),
        "leaders": leaders,
        "all_scores": scored[:8],
        "top_pick": top_pick,
        "updated_at": int(time.time()),
        "pdt_note": "Paper mode can still simulate PDT protections; live day-trading may be limited under $25k.",
    }


def main() -> int:
    print("stock_thinker.py is designed to be imported by the hub/runner first.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
