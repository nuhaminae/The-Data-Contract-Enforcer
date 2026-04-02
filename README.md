# The-Data-Contract-Enforcer

[![CI](https://github.com/nuhaminae/The-Data-Contract-Enforcer/actions/workflows/CI.yml/badge.svg)](https://github.com/nuhaminae/The-Data-Contract-Enforcer/actions/workflows/CI.yml)
![Black Formatting](https://img.shields.io/badge/code%20style-black-000000.svg)
![isort Imports](https://img.shields.io/badge/imports-isort-blue.svg)
![Flake8 Lint](https://img.shields.io/badge/lint-flake8-yellow.svg)

---

## Project Overview

The Data Contract Enforcer is a comprehensive framework designed to ensure data integrity, consistency, and compliance across complex data pipelines. It provides tools for defining data contracts, validating data against these contracts, attributing violations to specific components, analyzing schema evolution, and generating actionable reports for stakeholders. By automating the enforcement of data contracts, this project aims to enhance the reliability and maintainability of data systems while facilitating collaboration between data engineers, analysts, and business stakeholders.

---

## Key Features

- **Contract Generation**: Automatically generate YAML-based data contracts from various data sources, including intent records, extractions, lineage snapshots, events, and LangSmith traces.
- **Validation Runner**: Validate data outputs against defined contracts and produce structured validation reports.
- **Violation Attribution**: Attribute contract violations to specific components or stages in the data pipeline, providing detailed insights for debugging and resolution.
- **Schema Evolution Analysis**: Analyze changes in data schemas over time and assess their impact on contract compliance.
- **AI Contract Extensions**: Integrate AI-driven insights and recommendations into the contract enforcement process, enhancing the adaptability and intelligence of the framework.
- **Enforcer Report Generation**: Generate comprehensive reports for stakeholders, summarizing contract compliance, violations, and actionable insights for improving data quality and pipeline robustness.

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

- **Terminal Commands for Contract Generation (week 1 example)**

```bash
python contracts/generator.py --source outputs/week1/intent_records.jsonl --contract-id week1-intent-records --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/
```

- **Terminal Commands for Validation Runner (week 1 example)**
  
```bash
python contracts/runner.py --contract generated_contracts/week1-intent-records.yaml --data outputs/week1/intent_records.jsonl --output validation_reports/week1_validation.json
```

---

## Project Status

- Phase 0: Domain Reconnaissance and Initial Design (Completed)
- Phase 1: Contract Generation (Completed)
- Phase 2: Validation Runner and Violation Attribution (Partially Completed)
- Phase 3: Schema Evolution Analyser  (Upcoming)
- Phase 4: AI Contract Extensions Enforcer Report (Upcoming)

Check the [commit history](https://github.com/nuhaminae/The-Data-Contract-Enforcer/) for updates.
