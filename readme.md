# Permitting ETL Engine

This project implements a metadata-driven ETL pipeline to transform permitting system records into standardized `ProcessEventSet` JSON objects based on the [NR-PIES Specification](https://bcgov.github.io/nr-pies/docs/spec/element/message/process_event_set).

## Why Use This ETL Engine?

Traditionally, permitting ETL processes require hardcoded logic or complex reconfiguration of ETL tools whenever the mapping rules or lifecycle logic change. This ETL engine eliminates that friction by introducing a **metadata-driven**, **fully decoupled** design.

### Key Advantages

- **No More Hardcoding**  
    All process mapping logic is externalized into versioned JSON files. This means:
    - No changes to Python code
    - No updates to ETL Tool transformers or logic trees
    - No need to rebuild your workflow pipelines

- **Plug & Play with Any ETL Tool**  
    The engine is framework-agnostic. You can:
    - Run it inside **FME** (via PythonCaller)
    - Run it inside **Fabric Pipelines**
    - Call it from **Airflow**, **Azure Data Factory**, or other orchestration tools
    - Use it in **standalone scripts** or **API services**

- **Dynamic Lifecycle Mapping**  
    The lifecycle levels (`PHASE → STAGE → STATE`) are now dynamically loaded from `lifecycle_map.json`. This ensures your pipeline automatically adjusts to changes in business workflows.

- **Efficient + Maintainable**  
    By managing logic in human-readable JSON:
    - Business users and analysts can update mappings without touching code
    - Developers avoid frequent redeploys or script rewrites
    - Workflows remain clean, portable, and version-controlled  
    You don't need to manage the JSON directly. Instead, you can manage the mapping logic in a CSV file and use the tool available at [nr-etl-mapping-convertor](https://github.com/bcgov/nr-etl-mapping-convertor) to automatically convert it to JSON.

- **Time-Saving + Scalable**  
    Reduces update time from hours to minutes and scales easily across:
    - Multiple permitting systems
    - New schema versions
    - New jurisdictions or application processes

> This engine represents a shift from static ETL logic to **adaptive**, **configurable**, and **enterprise-ready** transformation workflows.

## Overview

This ETL Engine is designed to run in any ETL tool, with specific support for integration into FME workspaces and Fabric Pipelines. It applies rule-based logic to permit records and generates structured `process_event` output in accordance with NR-PIES specs.

### Key Inputs

- `rules.json`: Defines process event match conditions.
- `lifecycle_map.json`: Maps each rule to hierarchical process code levels (e.g., PHASE → STAGE → STATE).
- Source permit data: Typically from Oracle or CSV.

## How It Works

1. **Feature Extraction**  
     All incoming attributes from the feature are extracted and normalized.

2. **Rule Matching**  
     Each rule in `rules.json` is evaluated against the feature row.

3. **Event Generation**  
     Matching rules are transformed into `process_event` objects with dates, process codes, and statuses.

4. **JSON Output**  
     A `ProcessEventSet` JSON object is generated and saved to the specified output location.

## Directory Structure

```plaintext
.
├── FME/
│   ├── rules.json            # Mapping Rule definitions
│   ├── lifecycle_map.json    # Lifecycle mapping
│   ├── pythoncaller.py       # PythonCaller script for FME (FeatureProcessor class)
├── Fabric/
│   ├── mapping_logic/
│   │   ├── lifecycle_map.json    # Lifecycle mapping
│   │   ├── rules_land.json       # Land-specific rules
│   │   └── rules_water.json      # Water-specific rules
│   ├── notebook/
│       ├── lands_pies_id.py      # Land ETL logic
│       ├── water_authorizationid.py
│       ├── water_jobnumber.py
│       └── water_vfcbctrackingnumber.py
├── permit_etl_core.py        # Shared ETL logic (used by both FME and Fabric)
```

## JSON Output Format

Conforms to the [NR-PIES `ProcessEventSet`](https://bcgov.github.io/nr-pies/docs/spec/element/message/process_event_set) structure:

```json
{
    "transaction_id": "uuid",
    "version": "0.1.0",
    "kind": "ProcessEventSet",
    "system_id": "ITSM-5917",
    "record_id": "123456",
    "record_kind": "Permit",
    "on_hold_event_set": [],
    "process_event_set": [
        {
            "event": {
                "start_date": "YYYY-MM-DD",
                "end_date": "YYYY-MM-DD"
            },
            "process": {
                "code": "STATE_16",
                "code_display": "State 16",
                "code_set": [
                    "LIFECYCLE_16",
                    "APPLICATION_16",
                    "STAGE_16",
                    "STATE_16"
                ],
                "code_system": "https://bcgov.github.io/nr-pies/docs/spec/code_system/application_process",
                "status": "Some status",
                "status_code": "CODE",
                "status_description": "Optional description"
            }
        }
    ]
}
```

## 🛠 Requirements

- Python 3.11+
- Ensure `permit_etl_core.py`, `rules.json`, and `lifecycle_map.json` are placed in the appropriate directory for proper integration.
- JSON rules and lifecycle maps stored as UTF-8 files.

## How to Use in FME

1. Add a **PythonCaller** transformer.
2. Open the `pythoncaller.py` file and copy its entire content into the PythonCaller transformer.
3. Configure macro values:
     - `RulesFile` → path to `rules.json`
     - `LifecycleFile` → path to `lifecycle_map.json`
4. Output attribute: `json_output` (type: string).  
     Add an HTTPCaller transformer with the following configuration:
     - Endpoint URL: Specify the Hub API endpoint (e.g., `https://api.example.com/endpoint`).
     - HTTP Method: POST.
     - Headers: Include `Content-Type: application/json` and any required authentication headers (e.g., `Authorization: Bearer <token>`).
     - Body: Set the body to the `json_output` attribute.
     - Response Handling: Capture the response in an attribute (e.g., `response_output`) for further processing or logging.

## How to Use in Fabric Pipelines

1. Add the required notebook scripts to your Fabric pipeline.
2. Configure the pipeline to load the appropriate `rules.json` and `lifecycle_map.json` files.
3. Define the input source (e.g., Oracle, CSV) and output destination for the `ProcessEventSet` JSON.
4. Run the pipeline to process the data and generate the output.

## Optional: Use in Any ETL Tool

### Using the Universal ETL Logic Module

A standalone script leveraging the same core logic is available in `permit_etl_core.py`. This script can be integrated into automation tools such as Airflow, Azure Data Factory, or other ETL platforms. Additionally, you can use `run_etl.py` to orchestrate data transformations across various ETL platforms.

#### Output

The script will generate a `ProcessEventSet` JSON file based on the provided inputs and save it to the specified `output_file` path.

## Contact

For questions or contributions, reach out to the CSBC NR data team.
