# outputs/migrate/week1/migrate_records.py
# This script migrates Week 1 intent records from the old format (active_intents.yaml + agent_trace.jsonl) to the new format (intent_records.jsonl) used in Week 7.

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

# Get the script's current directory
current_script_path = Path(__file__).resolve()

# Go up levels to get to the project root
PROJECT_ROOT = current_script_path.parents[5]

print(f"Project root directory: {PROJECT_ROOT}")


# --- Load active intents ---
# Construct the full path from the dynamically found project root
intents_path = (
    PROJECT_ROOT / "week1" / "Roo-Code" / ".orchestration" / "active_intents.yaml"
)

print(f"Loading intents from: {intents_path}")
with open(intents_path) as f:
    intents = yaml.safe_load(f)["active_intents"]


# --- Load trace records ---
# Construct the full path from the dynamically found project root
trace_path = (
    PROJECT_ROOT / "week1" / "Roo-Code" / ".orchestration" / "agent_trace.jsonl"
)

print(f"Loading traces from: {trace_path}")
trace_records = []
with open(trace_path) as f:
    for line in f:
        trace_records.append(json.loads(line))

print("\nSuccessfully loaded files!")


# Group trace timestamps by intent_id
intent_timestamps = {}
for rec in trace_records:
    iid = rec["intent_id"]
    ts = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
    if iid not in intent_timestamps or ts < intent_timestamps[iid]:
        intent_timestamps[iid] = ts


# Governance tag mapping function
def infer_tags(intent):
    tags = set()
    text = (intent["name"] + " " + " ".join(intent.get("constraints", []))).lower()
    if "auth" in text or "jwt" in text or "middleware" in text:
        tags.add("auth")
    if (
        "scope" in text
        or "security" in text
        or "validation" in text
        or "approval" in text
    ):
        tags.add("security")
    if "governance" in text or "policy" in text:
        tags.add("governance")
    if "trace" in text or "log" in text or "hash" in text or "mutation" in text:
        tags.add("traceability")
    if "evolution" in text or "refactor" in text:
        tags.add("evolution")
    if not tags:
        tags.add("general")
    return list(tags)


# Build Week 7 intent_records.jsonl
output = []
for intent in intents:
    record = {
        "intent_id": intent["id"],
        "description": intent["name"]
        + " | "
        + "; ".join(intent.get("constraints", [])),
        "code_refs": [
            {
                "file": path,
                "line_start": 0,
                "line_end": 0,
                "symbol": None,
                "confidence": 1.0,
            }
            for path in intent.get("owned_scope", [])
        ],
        "governance_tags": infer_tags(intent),
        "created_at": intent_timestamps.get(
            intent["id"], datetime.now(timezone.utc)
        ).isoformat(),
    }
    output.append(record)

# Write JSONL
with open("outputs/week1/intent_records.jsonl", "w") as f:
    for rec in output:
        f.write(json.dumps(rec) + "\n")
