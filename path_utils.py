from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional, Tuple

_LOGGED_KEYS = set()


def log_once(key: str, message: str) -> None:
    if key in _LOGGED_KEYS:
        return
    _LOGGED_KEYS.add(key)
    try:
        print(message)
    except Exception:
        pass


def resolve_base_dir(module_file: str) -> str:
    return os.path.abspath(os.path.dirname(module_file))


def _normalize_candidate(base_dir: str, path_str: str) -> str:
    raw = str(path_str or "").strip()
    if not raw:
        return ""
    if os.path.isabs(raw):
        return os.path.abspath(raw)
    return os.path.abspath(os.path.join(base_dir, raw))


def resolve_settings_path(base_dir: str) -> Optional[str]:
    env_path = _normalize_candidate(base_dir, os.environ.get("POWERTRADER_GUI_SETTINGS", ""))
    root_path = os.path.join(base_dir, "gui_settings.json")
    hub_path = os.path.join(base_dir, "hub_data", "gui_settings.json")
    for candidate in (env_path, root_path, hub_path):
        if candidate and os.path.isfile(candidate):
            return candidate
    return None


def read_settings_file(path: Optional[str], module_name: str = "") -> Dict[str, Any]:
    if not path:
        if module_name:
            log_once(f"{module_name}:settings_missing", f"[{module_name}] warning: no gui_settings.json found")
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (FileNotFoundError, PermissionError, OSError, json.JSONDecodeError, ValueError) as exc:
        if module_name:
            log_once(
                f"{module_name}:settings_read:{path}:{type(exc).__name__}",
                f"[{module_name}] read_settings_file path={path} {type(exc).__name__}: {exc}",
            )
        return {}


def resolve_hub_data_dir(base_dir: str, settings_data: Optional[Dict[str, Any]] = None) -> str:
    settings_data = settings_data or {}
    configured = _normalize_candidate(base_dir, settings_data.get("hub_data_dir", ""))
    if configured and os.path.isdir(configured):
        hub_dir = configured
    else:
        hub_dir = os.path.join(base_dir, "hub_data")
    os.makedirs(hub_dir, exist_ok=True)
    return hub_dir


def resolve_runtime_paths(module_file: str, module_name: str = "") -> Tuple[str, Optional[str], str, Dict[str, Any]]:
    base_dir = resolve_base_dir(module_file)
    settings_path = resolve_settings_path(base_dir)
    settings_data = read_settings_file(settings_path, module_name=module_name)
    hub_data_dir = resolve_hub_data_dir(base_dir, settings_data)
    if module_name:
        log_once(
            f"{module_name}:runtime_paths",
            f"[{module_name}] SETTINGS_PATH={settings_path or 'None'} "
            f"HUB_DATA_DIR={hub_data_dir} BASE_DIR={base_dir}",
        )
    return base_dir, settings_path, hub_data_dir, settings_data
