# The-Data-Contract-Enforcer

[![CI](https://github.com/nuhaminae/The-Data-Contract-Enforcer/actions/workflows/CI.yml/badge.svg)](https://github.com/nuhaminae/The-Data-Contract-Enforcer/actions/workflows/CI.yml)
![Black Formatting](https://img.shields.io/badge/code%20style-black-000000.svg)
![isort Imports](https://img.shields.io/badge/imports-isort-blue.svg)
![Flake8 Lint](https://img.shields.io/badge/lint-flake8-yellow.svg)

---

## Project Overview

The Data Contract Enforcer is a comprehensive toolchain designed to automate the generation, validation, and governance of data contracts within a data engineering ecosystem. This project was developed as part of a structured, multi-phase initiative to establish robust data governance practices, ensuring data quality, lineage, and compliance across all data sources. The toolchain includes capabilities for migrating raw data into canonical formats, generating Bitol-compliant data contracts, validating data against those contracts, and producing consolidated reports on the health of the entire data landscape.

---

## Key Features

- **Data Migration**: A suite of Python scripts to migrate raw, weekly data (verdicts, extractions, events, lineage graphs) into a clean, canonical JSONL format.
- **Contract Generation**: Automatically generates Bitol-compliant data contracts and dbt model files from any JSONL data source.
- **Schema Validation**: Validates data sources against the schema defined in a data contract, checking for required fields, data types, and nullability.
- **AI-Powered Attribution**: Enriches intent records by using a lineage graph to attribute relevant code references to business-level intents.
- **AI-Powered Enrichment**: Uses a local LLM (via Ollama) to intelligently fill in missing descriptions in data contracts, ensuring they are always well-documented.
- **Schema & Compliance Reporting**: A full suite of reporting tools that analyze contract schemas and consolidate all validation results into a single, human-readable Markdown report.

---

## Table of Contents

- [The-Data-Contract-Enforcer](#the-data-contract-enforcer)
  - [Project Overview](#project-overview)
  - [Key Features](#key-features)
  - [Table of Contents](#table-of-contents)
  - [Project Structure (Snippet)](#project-structure-snippet)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
  - [Usage](#usage)
  - [Project Status](#project-status)

---

## Project Structure (Snippet)

```bash
The-Data-Contract-Enforcer/ 
├── contracts/ 
│   ├── generator.py           # ContractGenerator entry point 
│   ├── runner.py              # ValidationRunner entry point 
│   ├── attributor.py          # ViolationAttributor entry point 
│   ├── schema_analyser.py     # SchemaEvolutionAnalyser entry point 
│   ├── ai_extensions.py       # AI Contract Extensions entry point 
│   └── report_generator.py    # EnforcerReport entry point 
├── generated_contracts/       # OUTPUT: auto-generated YAML contract files 
│   ├── week1_intent_records.yaml 
│   ├── week3_extractions.yaml 
│   ├── week4_lineage.yaml 
│   ├── week5_events.yaml 
│   └── langsmith_traces.yaml 
├── validation_reports/        # OUTPUT: structured validation report JSON 
├── violation_log/             # OUTPUT: violation records JSONL 
├── schema_snapshots/          # OUTPUT: timestamped schema snapshots 
├── enforcer_report/           # OUTPUT: stakeholder PDF + data 
├── outputs/                   # INPUT: symlink or copy of your weeks 1–5 outputs 
│   ├── week1/intent_records.jsonl 
│   ├── week2/verdicts.jsonl 
│   ├── week3/extractions.jsonl 
│   ├── week4/lineage_snapshots.jsonl 
│   ├── week5/events.jsonl 
│   ├── traces/runs.jsonl      # from LangSmith export
│   └── migrate/             # historical outputs for schema evolution analysis 
└── DOMAIN_NOTES.md 
```

---

## Installation

### Prerequisites

- Python 3.12  
- Git  

---

### Setup

```bash
git clone https://github.com/nuhaminae/The-Data-Contract-Enforcer.git
cd The-Data-Contract-Enforcer
uv sync   # recommended dependency management
```

---

## Usage

- **Terminal Commands for Contract Generation**

```bash
python contracts/generator.py --source <path_to_input_data.jsonl> --contract-id <unique_contract_id> --lineage <path_to_lineage_data.jsonl> --output <path_to_output_contract.yaml>
# Example: Week 1: Intent-Code Correlator
python contracts/generator.py --source outputs/week1/intent_record.jsonl --contract-id week1-intent-records --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

- **Terminal Commands for Validation Runner**
  
```bash
python contracts/runner.py --contract <path_to_contract.yaml> --data <path_to_input_data.jsonl> --output <path_to_validation_report.json>
# Example: Week 1: Intent-Code Correlator
python contracts/runner.py --contract generated_contracts/week1-intent-records.yaml --data outputs/week1/intent_record.jsonl --output validation_reports/week1_validation.json
```

- **Terminal Commands for Violation Attribution**

```bash
python contracts/attributor.py --source <path_to_source_data.jsonl> --lineage <path_to_lineage_data.jsonl> --output <path_to_attributed_output.jsonl>
# Example:Week 1: Intent-Code Correlator
python contracts/attributor.py --source outputs/week1/intent_record.jsonl --lineage outputs/week4/lineage_snapshots.jsonl --output outputs/week1/intent_record_attributed.jsonl
```

- **Terminal Commands for Violation Creation**

```bash
python contracts/create_violation.py --input <path_to_clean_data.jsonl> --output <path_to_new_violated_data.jsonl>
python contracts/runner.py --contract <path_to_contract.yaml> --data <path_to_violated_data.jsonl> --output <path_to_validation_report.json>

# Example: Week 1: Intent-Code Correlator
python contracts/create_violation.py --input outputs/week1/intent_record.jsonl --output outputs/week1/intent_record_violated.jsonl
python contracts/runner.py --contract generated_contracts/week1-intent-records_dbt.yml --data outputs/week1/intent_record_violated.jsonl --output violation_log/week1_validation_FAILURE.json
```


- **Terminal Commands for Schema Evolution Analysis**

```bash
python contracts/schema_analyser.py --contract <path_to_input_contract.yaml> --output <path_to_output_report.json>
# Example: Week 1: Intent-Code Correlator
python contracts/schema_analyser.py --contract generated_contracts/week1-intent-records.yaml --output analysis_reports/week1_analysis.json
```

- **Terminal Commands for AI extensions and Enforcer Report Generation**

```bash
python contracts/ai_extension.py --input <path_to_source_contract.yaml> --output <path_to_new_enriched_contract.yaml>
# Example: Week 1: Intent-Code Correlator
python contracts/ai_extension.py --input generated_contracts/week1-intent-records.yaml --output generated_contracts/week1-intent-records_enriched.yaml
```

- **Terminal Commands for Enforcer Report Generation**

```bash
python contracts/report_generator.py --validation-dir <path_to_validation_reports> --analysis-dir <path_to_analysis_reports> --output <path_to_final_report.md>
python contracts/report_generator.py --validation-dir validation_reports/ --analysis-dir analysis_reports/ --output consolidated_summary_report.md
```

---

## Project Status

- Phase 0: Domain Reconnaissance and Initial Design (Completed)
- Phase 1: Contract Generation (Completed)
- Phase 2: Validation Runner and Violation Attribution (Completed)
- Phase 3: Schema Evolution Analyser  (Completed)
- Phase 4: AI Contract Extensions Enforcer Report (Completed)

Check the [commit history](https://github.com/nuhaminae/The-Data-Contract-Enforcer/) for updates.
