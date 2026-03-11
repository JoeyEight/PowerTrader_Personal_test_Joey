from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, List

from app.scan_diagnostics_schema import normalize_scan_diagnostics


def safe_read_json_dict(path: str) -> Dict[str, Any]:
    if not str(path or "").strip():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def safe_read_jsonl_dicts(path: str, limit: int = 200) -> List[Dict[str, Any]]:
    if not str(path or "").strip():
        return []
    rows: List[Dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                txt = str(ln or "").strip()
                if not txt:
                    continue
                try:
                    obj = json.loads(txt)
                except Exception:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
    except Exception:
        return []
    lim = max(1, int(limit or 200))
    return rows[-lim:]


def _is_missing_metric(value: Any) -> bool:
    txt = str(value or "").strip().lower()
    return txt in {"", "n/a", "pending account link", "none", "null"}


def payload_age_seconds(payload: Dict[str, Any], now_ts: float | None = None) -> float:
    if not isinstance(payload, dict):
        return float("inf")
    try:
        now = float(now_ts if now_ts is not None else time.time())
    except Exception:
        now = float(time.time())
    try:
        ts = float(payload.get("ts", payload.get("updated_at", 0)) or 0.0)
    except Exception:
        ts = 0.0
    if ts <= 0.0:
        return float("inf")
    return max(0.0, now - ts)


def market_status_has_account_snapshot(status_payload: Dict[str, Any]) -> bool:
    if not isinstance(status_payload, dict):
        return False
    for key in ("buying_power", "margin_available", "cash", "equity", "nav"):
        if not _is_missing_metric(status_payload.get(key)):
            return True
    raw_positions = status_payload.get("raw_positions", [])
    if isinstance(raw_positions, list) and raw_positions:
        return True
    positions_preview = status_payload.get("positions_preview", [])
    if isinstance(positions_preview, list) and positions_preview:
        return True
    return False


def needs_market_snapshot_refresh(
    status_payload: Dict[str, Any],
    loop_payload: Dict[str, Any],
    market_key: str,
    *,
    now_ts: float | None = None,
    stale_after_s: float = 45.0,
) -> bool:
    status = status_payload if isinstance(status_payload, dict) else {}
    loop = loop_payload if isinstance(loop_payload, dict) else {}
    market = str(market_key or "").strip().lower()
    threshold = max(5.0, float(stale_after_s or 45.0))
    status_age = payload_age_seconds(status, now_ts=now_ts)
    if math.isinf(status_age) or status_age > threshold:
        return True
    if not market_status_has_account_snapshot(status):
        return True
    state = str(status.get("state", "") or "").upper().strip()
    if state != "READY":
        return True
    try:
        loop_heartbeat_ts = float(loop.get("heartbeat_ts", loop.get("ts", 0)) or 0.0)
    except Exception:
        loop_heartbeat_ts = 0.0
    if market in {"stocks", "forex"} and loop_heartbeat_ts > 0.0:
        heartbeat_age = payload_age_seconds({"ts": loop_heartbeat_ts}, now_ts=now_ts)
        if heartbeat_age > threshold:
            return True
    return False


def load_market_status_bundle(
    *,
    status_path: str,
    trader_path: str,
    thinker_path: str,
    scan_diag_path: str,
    history_path: str = "",
    history_limit: int = 120,
    market_key: str = "",
) -> Dict[str, Any]:
    guessed_market = str(market_key or "").strip().lower()
    if not guessed_market:
        path_low = str(scan_diag_path or "").lower()
        if "/stocks/" in path_low or "\\stocks\\" in path_low:
            guessed_market = "stocks"
        elif "/forex/" in path_low or "\\forex\\" in path_low:
            guessed_market = "forex"
    return {
        "status": safe_read_json_dict(status_path),
        "trader": safe_read_json_dict(trader_path),
        "thinker": safe_read_json_dict(thinker_path),
        "scan_diagnostics": normalize_scan_diagnostics(safe_read_json_dict(scan_diag_path), market=guessed_market),
        "history": safe_read_jsonl_dicts(history_path, limit=history_limit),
    }
