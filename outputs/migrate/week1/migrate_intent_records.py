# outputs/migrate/week1/migrate_records.py
# This script migrates Week 1 intent records from the old format (active_intents.yaml) to the new format (intent_record.jsonl) used in Week 7.

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import yaml


def analyse_code_file(file_path_str, code_project_root):
    """
    Performs basic static analysis on a single source file.
    """
    # Construct the full path to the source code file
    full_path = code_project_root / file_path_str

    # --- DEBUGGING STEP ---
    # This line will print the exact path it's trying to open.
    print(f"    - Analysing path: {full_path}")
    # --- END DEBUGGING STEP ---

    try:
        content = full_path.read_text().splitlines()
        line_count = len(content)
        symbol_name = full_path.stem

        return {
            "line_start": 1,
            "line_end": line_count if line_count > 0 else 1,
            "symbol": symbol_name,
            "confidence": 0.8,
        }
    except FileNotFoundError:
        print(f"    - WARNING: Code file NOT FOUND at the path above.")
        return {
            "line_start": 1,
            "line_end": 1,
            "symbol": "placeholder_symbol (file not found)",
            "confidence": 0.1,
        }
    except Exception as e:
        print(f"    - ERROR: Could not read file. Error: {e}")
        return {
            "line_start": 1,
            "line_end": 1,
            "symbol": "placeholder_symbol (read error)",
            "confidence": 0.0,
        }


def migrate_intents():
    """
    Migrates intent records from YAML to the canonical JSONL format.
    """
    # Define the two key root directories explicitly.

    # The overall project root where the script is run from.
    PROJECT_ROOT = Path.cwd()

    # The specific root of the Week 1 'Roo-Code' project that contains the 'src' folder.
    current_script_path = Path(__file__).resolve()
    CODE_PROJECT_DIRECTORY = current_script_path.parents[5]
    CODE_PROJECT_ROOT = CODE_PROJECT_DIRECTORY / "week1" / "Roo-Code"

    # Define paths relative to the correct roots.
    input_yaml = CODE_PROJECT_ROOT / ".orchestration" / "active_intents.yaml"
    OUTPUT_DIR = PROJECT_ROOT / "outputs" / "week1"

    output_path = OUTPUT_DIR / "intent_record.jsonl"

    print(f"Overall Project Root: {PROJECT_ROOT}")
    print(f"Code Project Root:    {CODE_PROJECT_ROOT}")
    print(f"Input file:           {input_yaml}")
    print(f"Output file:          {output_path}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(input_yaml) as f:
        source_intents = yaml.safe_load(f)["active_intents"]

    migrated_records = []

    print("\nStarting migration...")
    for intent in source_intents:
        print(f"- Processing intent: {intent['id']}")

        code_refs = []
        if intent.get("owned_scope"):
            for file_path in intent["owned_scope"]:
                # Pass the root directory for the code analysis
                analysis_results = analyse_code_file(file_path, CODE_PROJECT_ROOT)
                code_refs.append({"file": file_path, **analysis_results})

        canonical_record = {
            "intent_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, intent["id"])),
            "description": f"{intent['name']} | {'; '.join(intent.get('constraints', []))}",
            "code_refs": code_refs,
            "governance_tags": ["general"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        migrated_records.append(canonical_record)

    with open(output_path, "w") as f_out:
        for record in migrated_records:
            f_out.write(json.dumps(record) + "\n")

    print(f"\nSuccessfully migrated {len(migrated_records)} intent records.")


if __name__ == "__main__":
    migrate_intents()
