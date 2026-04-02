# outputs/migrate/week3/migrate_extractions.py
# This script migrates Week 3 extraction records from the old format (inside .refinery/profiles, .refinery/extracted, and .refinery/extraction_ledger.jsonl)
# to the new format (extractions.jsonl) used in Week 7.


import json
from datetime import datetime
from pathlib import Path

# ------------- Path setup -------------
# Get the script's current directory
current_script_path = Path(__file__).resolve()

# Go up levels to get to the project root
PROJECT_ROOT = current_script_path.parents[5]

print(f"Project root directory: {PROJECT_ROOT}")


PROFILE_DIR = (
    PROJECT_ROOT / "week3" / "Document-Intelligence-Refinery" / ".refinery" / "profiles"
)
LEDGER_FILE = (
    PROJECT_ROOT
    / "week3"
    / "Document-Intelligence-Refinery"
    / ".refinery"
    / "extraction_ledger.jsonl"
)
EXTRACTED_DIR = (
    PROJECT_ROOT
    / "week3"
    / "Document-Intelligence-Refinery"
    / ".refinery"
    / "extracted"
)
OUTPUT_FILE = Path("outputs/week3/extractions.jsonl")


def load_profiles():
    profiles = {}
    for path in PROFILE_DIR.glob("*.json"):
        with open(path) as f:
            data = json.load(f)
            profiles[data["document_id"]] = data
    return profiles


def load_ledger():
    with open(LEDGER_FILE) as f:
        return [json.loads(line) for line in f if line.strip()]


def load_extracted():
    extracted = {}
    for path in EXTRACTED_DIR.glob("*.json"):
        with open(path) as f:
            data = json.load(f)
            extracted[data["document_id"]] = data
    return extracted


def consolidate(profiles, ledger, extracted):
    records = []
    for entry in ledger:
        doc_id = entry["document_id"]
        profile = profiles.get(doc_id, {})
        extract = extracted.get(doc_id, {})

        record = {
            "doc_id": doc_id,
            "source_path": profile.get("file_path"),
            "source_hash": profile.get("source_hash"),  # may be absent in your profiles
            "extracted_facts": extract.get("content_blocks", []),
            "entities": extract.get("entities", []),
            "extraction_model": entry.get("strategy_used"),
            "processing_time_ms": int(
                entry.get("cost_estimate", {}).get("runtime_sec", 0) * 1000
            ),
            "token_count": entry.get("cost_estimate", {}).get("tokens"),
            "extracted_at": datetime.utcnow().isoformat(),
        }
        records.append(record)
    return records


def write_jsonl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def run():
    profiles = load_profiles()
    ledger = load_ledger()
    extracted = load_extracted()
    records = consolidate(profiles, ledger, extracted)
    write_jsonl(records, OUTPUT_FILE)
    print(f"[DONE] Wrote {len(records)} records to {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
