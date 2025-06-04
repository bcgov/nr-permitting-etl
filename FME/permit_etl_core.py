#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# Copyright (c) 2025
# Province of British Columbia – Natural Resource Information & Digital Services
#
# ---------------------------------------------------------------------------
# permit_etl.py
# ---------------------------------------------------------------------------
# Transform permit source data (CSV or Parquet) into **ProcessEventSet**
# JSON Lines for the NR-PIES specification.
#
# The ETL engine chooses the fastest I/O backend available (pyarrow ▸ pandas ▸
# builtin csv) based on file size thresholds that are configurable via
# environment variables or CLI flags.
#
# ---------------------------------------------------------------------------

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
)
# from uuid6 import uuid7
import argparse
import csv
import gzip
import json
import logging
import operator
import os
import re
import sys
import time
import uuid
# ------------------------------------------------------------------
# Built-in default locations  (edit as you like)
# ------------------------------------------------------------------
# Remove DEFAULT_SOURCE_FILE      = "permits.csv" when running in FME or other ETL tool.
DEFAULT_SOURCE_FILE      = "permits.csv"
DEFAULT_RULES_FILE       = "rules.json"
DEFAULT_LIFECYCLE_FILE   = "lifecycle_map.json"
DEFAULT_OUTPUT_DIR       = "out"
DEFAULT_OUTPUT_FILENAME  = "events.jsonl"

# ─────────────────────────────────────────────────────────────────────────────
# Optional dependencies (fail-soft / import-probe once)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    USE_ARROW = True
except ImportError:  # pragma: no cover
    USE_ARROW = False

try:
    import pandas as pd

    USE_PANDAS = True
except ImportError:  # pragma: no cover
    USE_PANDAS = False

try:
    import orjson as _oj

    def _dumps(obj: Any) -> str:  # noqa: D401 – runtime optimisation
        """Serialize *obj* with **orjson** then decode to `str`."""
        return _oj.dumps(obj).decode()

except ImportError:  # pragma: no cover

    def _dumps(obj: Any) -> str:  # type: ignore[override]
        """Fallback JSON dump using stdlib `json`."""
        return json.dumps(obj, ensure_ascii=False)

try:
    from pythonjsonlogger import jsonlogger  # type: ignore

    JSON_LOGGER_AVAILABLE = True
except ImportError:  # pragma: no cover
    JSON_LOGGER_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
BYTES_IN_MIB = 1_048_576  # 1024**2


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────
class ETLError(RuntimeError):
    """Top-level exception for the ETL engine."""


# ─────────────────────────────────────────────────────────────────────────────
# Configuration dataclass
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    """Runtime configuration loaded from **ENV** then overridden by CLI flags.

    Env Vars
    --------
    ETL_SMALL_MIB : int, optional
        CSV files smaller than this (MiB) are eagerly read.
    ETL_MEDIUM_MIB : int, optional
        CSV files smaller than this (MiB) use chunk/batch mode.
    ETL_CHUNK_ROWS : int, optional
        Rows per chunk for pandas when streaming.
    LOG_LEVEL : str, optional
        Logging level string (INFO, DEBUG, …).
    LOG_JSON : int {0,1}, optional
        Emit structured JSON logs if the `python-json-logger` package is
        available (default 0).
    """

    small_mb: int = int(os.getenv("ETL_SMALL_MIB", "100"))
    medium_mb: int = int(os.getenv("ETL_MEDIUM_MIB", "200"))
    chunk_rows: int = int(os.getenv("ETL_CHUNK_ROWS", "100000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_json: bool = bool(int(os.getenv("LOG_JSON", "0")))

    # Convenience byte thresholds ------------------------------------------------
    @property
    def small_bytes(self) -> int:  # noqa: D401
        """Return eager-read threshold in *bytes*."""
        return self.small_mb * BYTES_IN_MIB

    @property
    def medium_bytes(self) -> int:
        """Return chunk/batch threshold in *bytes*."""
        return self.medium_mb * BYTES_IN_MIB


# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────
def _init_logging(cfg: Config, verbose: bool) -> None:
    """Configure root logger: plain text or structured JSON."""
    level = logging.DEBUG if verbose else getattr(
        logging,
        cfg.log_level.upper(),
        logging.INFO,
    )

    handler: logging.Handler
    if cfg.log_json and JSON_LOGGER_AVAILABLE:
        handler = logging.StreamHandler(sys.stdout)
        fmt = jsonlogger.JsonFormatter(
            (
                "%(asctime)s %(levelname)s %(name)s "
                "%(component)s %(message)s"
            ),
        )
        handler.setFormatter(fmt)
    else:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s %(message)s",
        )
        handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


logger = logging.getLogger("permit_etl")

# ─────────────────────────────────────────────────────────────────────────────
# Rule compilation helpers
# ─────────────────────────────────────────────────────────────────────────────
_OP: Mapping[str, Callable] = {
    "not_null": lambda x: x not in (None, "", "NULL"),
    "null": lambda x: x in (None, "", "NULL"),
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "in": lambda x, y: x in y,
    "regex": lambda x, rgx: rgx.search(str(x) or "") is not None,
}


def _coerce(value: Any) -> Any:  # noqa: ANN401 – JSON values are heterogenous
    """Coerce *YYYY-MM-DD* strings into `datetime` objects for comparisons."""
    if isinstance(value, str) and DATE_RE.match(value):
        try:
            return datetime.fromisoformat(value.replace("Z", ""))
        except ValueError:
            return value
    return value


def _compile_test(test: Dict[str, Any]) -> Callable[[Mapping], bool]:
    """Compile an atomic rule into a predicate function.

    :param test: Rule fragment with keys *op*, *attr*, *value* / *other_attr*.
    :return: Callable that accepts a row (Mapping) and returns bool.
    """
    op_name = test["op"]
    attr = test["attr"]
    op_func = _OP[op_name]
    value = test.get("value")
    other_attr = test.get("other_attr")

    if op_name == "regex" and isinstance(value, str):
        value = re.compile(value)

    def _inner(row: Mapping) -> bool:
        """Evaluate compiled test against *row*."""
        left = _coerce(row.get(attr))
        if op_name in ("null", "not_null"):
            return op_func(left)

        right = _coerce(row.get(other_attr)) if other_attr else value
        if left is None or right is None:  # do not match on missing data
            return False
        try:
            return op_func(left, right)
        except (TypeError, ValueError) as exc:
            logger.debug("Rule op %s failed attr %s: %s", op_name, attr, exc)
            return False

    return _inner


def _compile_logic(node: Dict[str, Any]) -> Callable[[Mapping], bool]:
    """Recursively compile *and/or* logic into a single predicate."""
    if "and" in node:
        parts = [
            _compile_logic(n)
            if {"and", "or"} & n.keys()
            else _compile_test(n)
            for n in node["and"]
        ]
        return lambda r: all(fn(r) for fn in parts)
    if "or" in node:
        parts = [
            _compile_logic(n)
            if {"and", "or"} & n.keys()
            else _compile_test(n)
            for n in node["or"]
        ]
        return lambda r: any(fn(r) for fn in parts)
    raise ETLError("Rule node missing 'and' or 'or' keys.")


# ─────────────────────────────────────────────────────────────────────────────
# Loaders
# ─────────────────────────────────────────────────────────────────────────────
def load_rules(path: Path, *, system: str | None = None) -> List[Dict]:
    """Load and compile rule definitions.

    :param path: Path to `rules.json`.
    :param system: Optional filter for the `"source"` field.
    :return: List of compiled rule dictionaries.
    """
    logger.info("Loading rules from %s", path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    compiled: List[Dict[str, Any]] = []
    for key, definition in raw.items():
        if (
            system
            and definition.get("source")
            and definition["source"].lower() != system.lower()
        ):
            continue
        start_attr = (
            (definition.get("start_date") or {}).get("attr")
            or definition.get("start_attr")
        )
        end_attr   = (
            (definition.get("end_date")   or {}).get("attr")
            or definition.get("end_attr")
        )
        compiled.append(
            {
                "key": key,
                "match": _compile_logic(definition["logic"]),
                "start": start_attr,
                "end": end_attr,
                "code_set": definition.get("code_set") or definition.get("class_path"),
            },
        )
    logger.info("Compiled %d rule entries", len(compiled))
    return compiled


def load_lifecycle(path: str | None) -> Dict[str, List[str]]:
    """Load lifecycle map if *path* is provided; otherwise return `{}`."""
    if not path:
        return {}
    lifecycle = json.loads(Path(path).read_text(encoding="utf-8"))
    logger.info("Loaded lifecycle map (%d items)", len(lifecycle))
    return lifecycle


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────
def _file_size(path: Path) -> int:
    """Return file size in bytes."""
    return path.stat().st_size


def _iter_csv(path: Path, cfg: Config) -> Iterator[Dict[str, Any]]:
    """Yield CSV rows using the smartest backend for *path*.

    Decision matrix
    ---------------
    - **pyarrow** available:
        * `< cfg.medium_bytes`   → eager read via Table.to_pylist()
        * `>= medium_bytes`      → streaming batches
    - **pandas** available:
        * `< cfg.small_bytes`    → eager DataFrame
        * else                   → chunked read
    - Fallback: builtin `csv.DictReader`
    """
    size = _file_size(path)
    log_ctx = {"component": "csv_reader", "bytes": size}

    # ── pyarrow path ─────────────────────────────────────────────────────────
    if USE_ARROW:
        if size < cfg.medium_bytes:
            logger.debug("pyarrow eager", extra=log_ctx)
            import pyarrow.csv as pacsvmod
            parse_opts = pacsvmod.ParseOptions(newlines_in_values=True)
            table = pacsv.read_csv(str(path), parse_options=parse_opts)

            for row in table.to_pylist():
                yield row
        else:
            logger.debug("pyarrow batches", extra=log_ctx)
            for batch in pacsv.read_csv(str(path), parse_options=parse_opts).to_batches():
                data = batch.to_pydict()
                for i in range(len(batch)):
                    yield {k: v[i] for k, v in data.items()}
        return

    # ── pandas path ──────────────────────────────────────────────────────────
    if USE_PANDAS:
        logger.debug("pandas", extra=log_ctx)
        reader: Iterable[Any]
        if size < cfg.small_bytes:
            reader = [pd.read_csv(path, dtype=str)]
        else:
            reader = pd.read_csv(path, dtype=str, chunksize=cfg.chunk_rows)
        for chunk in reader:
            for row in chunk.to_dict(orient="records"):
                yield row
        return

    # ── builtin fallback ────────────────────────────────────────────────────
    logger.debug("builtin csv", extra=log_ctx)
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            yield row


def _iter_parquet(path: Path) -> Iterator[Dict[str, Any]]:
    """Yield Parquet rows in streaming batches (requires `pyarrow`)."""
    if not USE_ARROW:
        raise ETLError("Parquet support requires pyarrow; please install it.")
    logger.debug("parquet batches", extra={"component": "parquet_reader"})
    for batch in pq.ParquetFile(str(path)).iter_batches():
        data = batch.to_pydict()
        for i in range(len(batch)):
            yield {k: v[i] for k, v in data.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Writer
# ─────────────────────────────────────────────────────────────────────────────
def _open_outfile(out_path: Path):
    """Return a writable handle; gzip transparently if `.gz` suffix."""
    if out_path.suffix == ".gz":
        return gzip.open(out_path, "wt", encoding="utf-8")  # type: ignore[arg-type]
    return out_path.open("w", encoding="utf-8")


def write_jsonl(
    events: Iterable[Dict[str, Any]],
    out_dir: Path,
    outfile: str = "events.jsonl",
) -> None:
    """Write *events* to `out_dir/outfile` (optionally `.gz`)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / outfile
    with _open_outfile(out_path) as fh:
        for event in events:
            fh.write(_dumps(event) + "\n")
    logger.info("Wrote %s", out_path)


# ─────────────────────────────────────────────────────────────────────────
# Code-set resolver  (any length list)
# ─────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────
# Process-info resolver  (dynamic code_set order)
# ─────────────────────────────────────────────────────────────────────────
def _resolve_process_info(
    status_key: str,
    rule_code_set: Any,
    lifecycle_map: Mapping[str, Any],
) -> tuple[list[str], dict[str, str]]:
    """
    Return ``(code_set_list, status_dict)`` with NO hard-coded level names.

    Priority
    --------
    1. If *rule* supplies ``code_set`` (list / dict / "/" string) → use it.
    2. Else look up *status_key* in ``lifecycle_map``:
       • If entry is *dict*:   code_set = values() in their JSON order,
                               status   = entry.get("status", {})
       • If entry is *list*:   treat list itself as code_set.
       • If entry is *str*:    single-element code_set.
    3. Fallback → (["UNKNOWN"], {})
    """
    # ── rule override ─────────────────────────────────────────────────────
    if rule_code_set is not None:
        if isinstance(rule_code_set, list):
            return [str(x).strip() for x in rule_code_set if x], {}
        if isinstance(rule_code_set, dict):
            return [str(v).strip() for v in rule_code_set.values() if v], {}
        # accept "LEVEL1/LEVEL2/LEVEL3" string
        return [seg.strip() for seg in str(rule_code_set).split("/") if seg], {}

    # ── lifecycle lookup ─────────────────────────────────────────────────
    entry = lifecycle_map.get(status_key)
    if entry is None:
        return ["UNKNOWN"], {}

    # New style object: {"status":{...}, "code_set":{...}}
    if isinstance(entry, dict):
        cs_dict  = entry.get("code_set", {})
        # dict preserves order since Python 3.7+
        code_set = [str(v).strip() for v in cs_dict.values() if v] or ["UNKNOWN"]
        return code_set, entry.get("status", {})

    # Legacy list style
    if isinstance(entry, list):
        return [str(x).strip() for x in entry if x], {}

    # Simple string fallback
    if isinstance(entry, str):
        return [entry.strip()], {}

    # Anything unexpected → fallback
    return ["UNKNOWN"], {}



def _safe_get(row: Mapping, col: str | None) -> str | None:
    """Return cell value as *str* or None (if col missing / empty)."""
    if not col:
        return None
    val = row.get(col)
    if val in (None, "", "NULL"):
        return None
    return str(val)

def _to_date_str(value: str | None) -> str | None:
    """Convert datetime or timestamp string to 'YYYY-MM-DD' (or None)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value[:10]).date().isoformat()
    except Exception:
        return None  # fallback: ignore invalid dates

# ─────────────────────────────────────────────────────────────────────────────
# Event builders
# ─────────────────────────────────────────────────────────────────────────────
def _build_event(
    row: Mapping[str, Any],
    rule: Mapping[str, Any],
    lifecycle_map: Mapping[str, Any],
) -> Dict[str, Any]:
    """Create one process_event compliant with the 2024-12 schema."""
    status_key           = rule["key"]
    code_set, stat_info  = _resolve_process_info(
        status_key,
        rule.get("code_set"),
        lifecycle_map,
    )

    # --- event dates (omit if blank) ------------------------------------
    start_val = _to_date_str(_safe_get(row, rule["start"]))
    end_val   = _to_date_str(_safe_get(row, rule["end"]))
    event_blk: Dict[str, Any] = {}
    if start_val:
        event_blk["start_date"] = start_val
    if end_val:
        event_blk["end_date"]   = end_val


    # --- process block ---------------------------------------------------
    proc_blk: Dict[str, Any] = {
        "code":         code_set[-1],
        "code_display": code_set[-1].replace("_", " ").title(),
        "code_set":     code_set,
        "code_system": (
            "https://bcgov.github.io/nr-pies/docs/spec"
            "/code_system/application_process"
        ),
    }
    # optional status pieces
    if stat_info:
        if stat := stat_info.get("STATUS"):
            proc_blk["status"] = stat
        if scode := stat_info.get("status_code"):
            proc_blk["status_code"] = scode
        if desc := stat_info.get("status_description"):
            proc_blk["status_description"] = desc
            
    return {"event": event_blk, "process": proc_blk}




def _build_pes(row: Mapping[str, Any], events: List[Mapping]) -> Dict[str, Any]:
    """Create a *ProcessEventSet* wrapper for *events*."""
    return {
        "transaction_id": str(uuid.uuid4()),
        # "transaction_id": str(uuid7()),
        "version": "0.1.0",
        "kind": "ProcessEventSet",
        "system_id": row.get("system_id", "ITSM-5917"),
        "record_id": str(
            row.get("AUTHORIZATION_ID") or row.get("PROJECT_ID") or "Error record_id"
        ),
        "record_kind": "Permit",
        "process_event": events,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core engine
# ─────────────────────────────────────────────────────────────────────────────
def smart_engine(
    src: Path,
    fmt: str,
    rules: List[Mapping],
    class_map: Mapping[str, List[str]],
    out_dir: Path,
    outfile: str,
    cfg: Config,
) -> None:
    """Run the ETL pipeline.

    :param src: Source file path.
    :param fmt: `"csv"` or `"parquet"`.
    :param rules: Compiled rule objects.
    :param class_map: Lifecycle classification mapping.
    :param out_dir: Directory for output.
    :param outfile: Output filename (may end with `.gz`).
    :param cfg: `Config` instance.
    """
    logger.info("Processing %s (%s)", src, fmt)
    reader: Callable[[Path], Iterable[Dict[str, Any]]]
    if fmt == "csv":
        reader = lambda p: _iter_csv(p, cfg)
    elif fmt == "parquet":
        reader = _iter_parquet
    else:
        raise ETLError(f"Unknown format {fmt!r}")

    start = time.perf_counter()
    row_count = event_count = 0

    def _row_gen() -> Iterator[Dict[str, Any]]:
        nonlocal row_count, event_count
        for row in reader(src):
            row_count += 1
            evts = [
                _build_event(row, rule, class_map)
                for rule in rules
                if rule["match"](row)
            ]
            if evts:
                event_count += len(evts)
                yield _build_pes(row, evts)

    write_jsonl(_row_gen(), out_dir, outfile)

    dur = time.perf_counter() - start
    logger.info(
        "Rows: %d  Events: %d  Elapsed: %.2fs",
        row_count,
        event_count,
        dur,
    )
