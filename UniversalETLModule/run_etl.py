"""
run_etl.py  core utility you can import anywhere for permit ETL
-------------------------------------------------
Exposes small functions that:

* load rules / lifecycle JSON
* normalise raw rows (safe types, date parsing)
* build ProcessEventSet dicts (via permit_etl_core)

Designed to be reused by FME PythonCaller, Airflow, Glue, Fabric, CLI, etc.
"""
from __future__ import annotations
import math, json
from datetime import datetime, date
from pathlib import Path
from typing import List, Mapping, Any

import permit_etl_core as etl   # unchanged core lib

# ── safe-type helpers ──────────────────────────────────────────────
def _safe_attr(val: Any) -> Any:
    """Coerce to JSON-safe primitives."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()[:10]                # YYYY-MM-DD
    if isinstance(val, (bytes, bytearray)):
        try:    return val.decode("utf-8", errors="ignore")
        except: return val.hex()
    if isinstance(val, (str, int, float, bool, list, dict)):
        return val
    return str(val)

def _normalize_date_str(raw: Any) -> Any:
    """Convert Oracle/FME style 20230602000000 → 2023-06-02."""
    if isinstance(raw, str) and raw.isdigit() and len(raw) >= 8:
        try:
            return datetime.strptime(raw[:8], "%Y%m%d").strftime("%Y-%m-%d")
        except Exception:
            return raw
    return raw

# ── public api ─────────────────────────────────────────────────────
def load_rules_and_lifecycle(rules_path: str|Path,
                             lifecycle_path: str|Path) -> tuple[list, dict]:
    rules      = etl.load_rules(Path(rules_path))
    lifecycle  = etl.load_lifecycle(Path(lifecycle_path))
    return rules, lifecycle

def process_row(raw_row: Mapping[str, Any],
                rules: List[Mapping],
                lifecycle: Mapping[str, Any]) -> dict|None:
    """Return one ProcessEventSet dict or None (if no rule matched)."""
    row = {k: _normalize_date_str(_safe_attr(v)) for k, v in raw_row.items()}
    events = [
        etl._build_event(row, rule, lifecycle)
        for rule in rules if rule["match"](row)
    ]
    return etl._build_pes(row, events) if events else None

def process_rows(rows_iter, rules_path, lifecycle_path):
    """Yield PES dicts for an iterator of raw rows (dict-like)."""
    rules, lifecycle = load_rules_and_lifecycle(rules_path, lifecycle_path)
    for raw in rows_iter:
        result = process_row(raw, rules, lifecycle)
        if result:
            yield result
