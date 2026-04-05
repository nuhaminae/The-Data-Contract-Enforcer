# outputs/migrate/week5/migrate_events.py
# This script migrates Week 5 audit events from the old format (events.jsonl) to the new format (event_record.jsonl) used in Week 7.

import json
import uuid
from pathlib import Path

# ------------- Path setup -------------
# Get the script's current directory
try:
    current_script_path = Path(__file__).resolve()
    # Go up levels to get to the project root
    PROJECT_ROOT = current_script_path.parents[5]
except NameError:
    PROJECT_ROOT = Path.cwd()


print(f"Project root directory: {PROJECT_ROOT}")

SEED_FILE = (
    PROJECT_ROOT
    / "week5"
    / "Agentic-Event-Store-Enterprise-Audit-Infrastructure"
    / "data"
    / "seed_events.jsonl"
)

# Constructing output path relative to the project root
OUTPUT_DIR = PROJECT_ROOT / "week7" / "The-Data-Contract-Enforcer" / "outputs" / "week5"
OUTPUT_FILE = OUTPUT_DIR / "event_record.jsonl"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"Input seed file: {SEED_FILE}")
print(f"Output file: {OUTPUT_FILE}")


def infer_aggregate_type(event_type):
    """Infers the aggregate type based on the event type string."""
    if "Application" in event_type:
        return "Application"
    if "Loan" in event_type:
        return "Loan"
    # Add more rules here as needed
    return "Unknown"


def migrate_event(source_event):
    """
    Transforms a single source event into the new canonical event structure.
    """
    payload = source_event.get("payload", {})

    # Generate new UUIDs for correlation and the event itself
    correlation_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())

    # Determine aggregate root and type
    aggregate_id = payload.get("application_id")
    aggregate_type = infer_aggregate_type(source_event.get("event_type"))

    # If there's no clear aggregate ID in the payload, we'll use the correlation ID
    if not aggregate_id:
        aggregate_id = correlation_id
        aggregate_type = "SystemProcess"

    canonical_event = {
        "event_id": event_id,
        "event_type": source_event.get("event_type", "UnknownEvent"),
        "aggregate_id": aggregate_id,
        "aggregate_type": aggregate_type,
        "sequence_number": source_event.get("event_version", 1),
        "payload": payload,
        "metadata": {
            "causation_id": None,  # Not available in the source data
            "correlation_id": correlation_id,
            "user_id": payload.get("applicant_id", "system"),
            "source_service": "legacy-submission-service",  # A descriptive name for the origin
        },
        "schema_version": "1.0",
        "occurred_at": payload.get(
            "submitted_at", source_event.get("recorded_at")
        ),  # Best guess for when it happened
        "recorded_at": source_event.get("recorded_at"),
    }
    return canonical_event


# --- Main Execution ---
if not SEED_FILE.exists():
    print(f"Error: Input file not found at {SEED_FILE}")
else:
    migrated_events = []
    with open(SEED_FILE, "r", encoding="utf-8") as f_in:
        for line in f_in:
            try:
                source_event = json.loads(line)
                migrated_event = migrate_event(source_event)
                migrated_events.append(migrated_event)
            except json.JSONDecodeError:
                print(f"Warning: Skipping malformed JSON line: {line.strip()}")

    if migrated_events:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f_out:
            for event in migrated_events:
                f_out.write(json.dumps(event) + "\n")

        print(f"\n✅ Success! Migrated {len(migrated_events)} events.")
        print(f"Output written to: {OUTPUT_FILE}")
    else:
        print("\nNo events were migrated. Check the input file for content.")
