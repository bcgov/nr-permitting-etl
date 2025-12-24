# Permitting ETL Engine

The **Permitting ETL Engine** is a **metadata-driven Python ETL framework** that transforms permitting system records into standardized **NR-PIES `ProcessEventSet` JSON** objects, ready for downstream consumers such as **PEACH**.

This repo is designed to **separate business mapping logic from ETL tooling**, so that changes to event rules and lifecycle structures can be made by updating mapping files (JSON) rather than rewriting code or rebuilding ETL workflows.

> NR-PIES Specification: https://bcgov.github.io/nr-pies/docs/intro

---

## Table of Contents

- [Permitting ETL Engine](#permitting-etl-engine)
  - [What This Repo Does](#what-this-repo-does)
  - [Key Benefits](#key-benefits)
  - [Architecture (High Level)](#architecture-high-level)
  - [Inputs and Mapping Files](#inputs-and-mapping-files)
  - [How the Engine Works](#how-the-engine-works)
  - [Repository Structure](#repository-structure)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Option A: Standalone Python](#option-a-standalone-python)
    - [Option B: FME (PythonCaller)](#option-b-fme-pythoncaller)
    - [Option C: Microsoft Fabric](#option-c-microsoft-fabric)
    - [Option D: Other Orchestrators (Airflow / ADF)](#option-d-other-orchestrators-airflow--adf)
  - [Outputs](#outputs)
  - [Operational Notes](#operational-notes)
  - [Logging and Validation](#logging-and-validation)
  - [Extending the Engine](#extending-the-engine)
  - [Roadmap](#roadmap)
  - [Getting Help / Reporting Issues](#getting-help--reporting-issues)
  - [Contributing](#contributing)
  - [License](#license)

---

## What This Repo Does

This repository provides:

- A **Python transformation engine** that reads:
  - **source permitting data** (rows / features)
  - **rules.json** (event detection logic)
  - **lifecycle_map.json** (rule → lifecycle class mapping)

- And produces:
  - **NR-PIES ProcessEventSet JSON** objects
  - Optionally **JSONL** files, **validation logs**, and **push-ready payloads** for PEACH or other APIs

---

## Key Benefits

- **Metadata-driven**: rules are externalized into JSON files (auditable + version-controlled).
- **Framework-agnostic**: same business logic works across Fabric, FME, Airflow, Azure Data Factory, etc.
- **Low-maintenance**: update mapping JSON rather than editing pipelines or hardcoding logic.
- **Scalable**: supports multiple domains (Water, Lands, ATS, etc.) and future systems.
- **Consistent output**: all domains output the same NR-PIES structure.

---

## Architecture (High Level)

The engine is typically used as part of a layered ETL architecture:

- **L0** Source Systems (Oracle views, business system extracts, etc.)
- **L1** Ingestion into a landing/raw layer
- **L2** Dataflow / transformations (cleaning, joins, DQ checks) → curated tables
- **L3** Curated Lakehouse / “Gold” tables (trusted inputs)
- **L4** **This repo**: apply `rules.json` + `lifecycle_map.json` to generate ProcessEventSet JSON
- **L5** Outputs: JSONL + validation/logs + API push (e.g., PEACH)
- **L6** Historical archives for audit/replay/backfill

> When onboarding a new business system, the pattern stays the same.  
> Only the **source data** and **L2 transformation/join logic** are system-specific.

---

## Inputs and Mapping Files

### `rules.json`

Defines the **event detection rules** used to create `process_events`.

Typical rule responsibilities:
- Identify whether an event exists for a record (based on field presence / conditions)
- Choose event date fields (`start_date`, optional `end_date`)
- Provide event metadata for downstream mapping

### `lifecycle_map.json`

Maps each rule key to the **NR-PIES lifecycle class path**, typically a 3–4 level hierarchy.

Example concepts:
- PHASE → STAGE → STATE
- The engine uses this to populate the `class` field on each `process_event_set`.

### Source Data

Source records are typically:
- Oracle database/views
- Fabric Lakehouse curated tables

**Important**: The engine assumes your upstream ETL step has:
- standardized key fields
- consistent date formats where possible
- cleaned nulls / types
- produced join-ready curated outputs

---

## How the Engine Works

At runtime, the engine executes the following workflow per record:

1. **Normalize input attributes**
   - Trim text, standardize nulls, interpret dates
   - Ensure expected keys exist (when missing, logic should fail gracefully)

2. **Evaluate rules**
   - Loop through each rule in `rules.json`
   - Determine if the record matches (e.g., field exists, conditions satisfied)

3. **Generate events**
   - For each matching rule:
     - Compute `start_date` (and optional `end_date`)
     - Map the rule to a lifecycle class via `lifecycle_map.json`
     - Build a `process_event` entry

4. **Assemble ProcessEventSet**
   - Collect process events into a single `ProcessEventSet`
   - Attach identifiers and key metadata
   - Output as JSON (or JSONL line)

---

## Usage

### Option A: Standalone Python

Use this option if you want to run locally, in a batch job, or in an orchestrator.

Example (illustrative):

```bash
python run_etl.py \
  --input-file ./data/source.parquet \
  --rules ./mappings/rules.json \
  --lifecycle ./mappings/lifecycle_map.json \
  --output ./out/process_event_set.jsonl
```

Typical behaviors:
- Read input records from CSV/Parquet.
- Produce one JSON object per record (JSONL).
- Emit validation logs if configured.

If your repo does not include `run_etl.py`, call `permit_etl_core.py` from your own wrapper.

---

### Option B: FME (PythonCaller)

1. Add a **PythonCaller** transformer.
2. Copy the entire content of `pythoncaller.py` into the PythonCaller.
3. Set macro values:
   - `RulesFile` → path to `rules.json`
   - `LifecycleFile` → path to `lifecycle_map.json`
4. Output attribute:
   - `json_output` (string)

#### Push to API (PEACH or other service)

1. Add an **HTTPCaller** transformer.
2. Example configuration:
   - **Method**: POST
   - **Headers**:
     - `Content-Type: application/json`
     - `Authorization: Bearer <token>`
   - **Body**: `json_output`
   - **Store response in**: `response_output`

---

### Option C: Microsoft Fabric

Typical Fabric usage:
1. Ingest source data into Fabric Lakehouse (L1).
2. Use Dataflow Gen2 to:
   - Clean
   - Normalize
   - Join
   - Apply DQ checks (L2)
3. Write curated outputs into trusted curated tables (L3).
4. Run a Fabric Notebook to execute the ETL engine (L4):
   - Load curated table rows.
   - Load `rules.json` and `lifecycle_map.json`.
   - Generate `ProcessEventSet` JSON.
5. Emit outputs (L5):
   - JSONL files for replay/audit.
   - Validation logs.
   - Optional PEACH API push.
6. Archive outputs for historical retention (L6).

---

### Option D: Other Orchestrators (Airflow / ADF)

The ETL engine is a plain Python module and can be embedded into:
- **Airflow DAG tasks**
- **Azure Data Factory custom activities**
- **Container jobs / scheduled compute**

#### Recommended pattern:
- Orchestrator handles scheduling + data movement.
- Engine handles only transformation + schema output.

---

## Outputs

### ProcessEventSet JSON

Primary output is a `ProcessEventSet` JSON object per record, containing:
- Record identifier(s)
- `process_events_set` array
- Lifecycle class paths per event
- Event dates and metadata

### JSONL (Recommended)

For operational pipelines, JSONL is recommended:
- One JSON object per line.
- Easier replay/backfill.
- Supports large-scale processing.

### Logs / Validation Results (Optional)

Depending on integration:
- Validation pass/fail flags.
- Missing field diagnostics.
- Rule hit/miss counts.
- Per-record error capture.

---

## Operational Notes

### Record Linkage / Identifiers (Water Example)

Some domains do not yet have a record linkage system. In those cases, ID selection may be priority-based.

Example (Water, temporary):
1. Use Job Number if present.
2. Else use VFCBC Tracking ID.
3. Else fall back to Authorization ID.

This is a temporary solution until a record linkage system exists, at which point a single unique ID will be used.

---

## Logging and Validation

### Recommended best practices:
- Structured logging with record identifiers included.
- Track:
  - Number of records processed.
  - Number of events generated.
  - Rule hit counts.
  - Validation failures and reasons.
- Write logs alongside JSONL outputs to support:
  - Audit.
  - Replay.
  - Troubleshooting.
  - Operational monitoring.

---

## Extending the Engine

To add support for a new permitting domain or business system:
1. Build the curated dataset upstream (clean + join + DQ checks).
2. Create or update:
   - `rules.json`
   - `lifecycle_map.json`
3. Validate output against NR-PIES.
4. Deploy updated mappings (no code change required unless new rule operators are needed).

---

## Roadmap

- Improve validation coverage and error reporting.
- Add more examples for Fabric.
- Add automated tests for rule evaluation.
- Add mapping conversion utilities (CSV → JSON) where applicable.
- Enhance documentation for business users.

---

## Getting Help / Reporting Issues

Contact: **CSBC NR Data Team**

Or open an issue in this repository with:
- Domain (Water / Lands / etc.)
- Sample record identifiers.
- Expected vs actual events.
- Mapping file version used.

---

## Contributing

1. Fork the repo.
2. Create a branch for your change.
3. Add/modify mappings and/or code.
4. Submit a PR with a clear description and test evidence.

---

## License

### Code License (Apache 2.0)

```plaintext
Copyright 2025 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at 

   http://www.apache.org/licenses/LICENSE-2.0
```

### Documentation License (CC BY 4.0)

Documentation and non-code content are licensed under:
[Creative Commons Attribution 4.0 International License](https://creativecommons.org/licenses/by/4.0/)