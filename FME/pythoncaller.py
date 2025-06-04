# ── FME PythonCaller script for ETL permit data ───────────────────────
# ── Description: This script is used in the FME PythonCaller transformer
# ── to process permit data and generate JSON output.
# ── README / Help Documentation ──────────────────────────────────────
# This script is designed to be used within the FME PythonCaller transformer.
# It processes permit data and generates JSON output based on specific rules
# and lifecycle configurations. Below is a brief guide to its usage:

# 1. Prerequisites:
#    - Ensure the `permit_etl_core.py`, 'rules.json' and 'lifecycle_map.json' are available in the specified path.
#    - Provide valid `RulesFile` and `LifecycleFile` paths via FME macro values(User attributes).
#    - An AttributeExposer transformer must be placed after the PythonCaller.

# 2. Key Components:
#    - `FeatureProcessor`: Main class handling feature processing.
#    - `input`: Processes each feature, applies rules, and generates JSON output.
#    - `close`: Finalizes processing and logs summary statistics.

# 3. Outputs:
#    - Adds a `json_output` attribute to features containing the generated JSON.

# 4. Notes:
#    - The transformer AttributeExposer must be placed after the PythonCaller
#      to expose the `json_output` attribute for further use in the workflow.

# 5. Logging:
#    - Logs initialization, example outputs, and summary statistics to FME logs.

# 6. Example Usage:
#    - Place `permit_etl_core.py`, 'rules.json' and 'lifecycle_map.json' to your local path.
#    - Change the path in `sys.path.append(r"C:\Users\W1ZHANG\Documents\FME\Workspaces")`
#    - Place this script in the PythonCaller transformer.
#    - Configure the `RulesFile` and `LifecycleFile` paths in the transformer.
#    - Change the name of RulesFile and LifecycleFile if you have different names. e.g.LandRules, LandLifecycle.
#    - Use AttributeExposer to make the `json_output` attribute available.
# ------------------ PythonCaller: FeatureProcessor ------------------
import sys, math, json
from pathlib import Path
from datetime import date, datetime

import fme
import fmeobjects  # for logging + FME_NULL

# Add folder that holds permit_etl_core.py
sys.path.append(r"C:\Users\W1ZHANG\Documents\FME\Workspaces")
import permit_etl_core as etl  # (unchanged!)

# ── FME logger -----------------------------------------------------
_log = fmeobjects.FMELogFile()
def log(msg): _log.logMessageString(msg, fmeobjects.FME_INFORM)

# ── Local fallback for FME_NULL if not available -------------------
_FME_NULL_SENTINEL = object()
FME_NULL = getattr(fmeobjects, "FME_NULL", _FME_NULL_SENTINEL)

# ── Normalize values (safe for JSON, rule matching, date fix) ------
def _safe_attr(val):
    """Convert raw FME attribute value to safe Python type."""
    if val is FME_NULL or val is _FME_NULL_SENTINEL or val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, (datetime, date)):
        return val.isoformat()[:10]
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8", errors="ignore")
        except Exception:
            return val.hex()
    if isinstance(val, (str, int, float, bool, list, dict)):
        return val
    return str(val)

def _normalize_date_str(raw):
    """Convert '20230602000000' → '2023-06-02' if needed."""
    try:
        if isinstance(raw, str) and raw.isdigit() and len(raw) >= 8:
            return datetime.strptime(raw[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        pass
    return raw  # fallback

# ── Main Feature Processor ------------------------------------------
class FeatureProcessor:
    def __init__(self):
        self.rules     = etl.load_rules(Path(fme.macroValues["RulesFile"]))
        self.lifecycle = etl.load_lifecycle(Path(fme.macroValues["LifecycleFile"]))
        self.n_in = self.n_out = 0
        log(f"[PyCaller] Init – {len(self.rules)} rules loaded")

    def input(self, feature):
        self.n_in += 1

        # Coerce all attributes
        raw_row = {n: feature.getAttribute(n) for n in feature.getAllAttributeNames()}
        row = {}
        for k, v in raw_row.items():
            val = _safe_attr(v)
            row[k] = _normalize_date_str(val)

        evts = [
            etl._build_event(row, rule, self.lifecycle)
            for rule in self.rules
            if rule["match"](row)
        ]

        if not evts:
            return

        pes_json = json.dumps(etl._build_pes(row, evts))
        feature.setAttribute("json_output", pes_json)
        self.pyoutput(feature)

        self.n_out += 1
        if self.n_out <= 3:
            log(f"[PyCaller] Example json_output → {pes_json[:120]}…")

    def close(self):
        log(f"[PyCaller] Done – {self.n_in} rows processed, {self.n_out} emitted")
