# outputs/migrate/week5/migrate_events.py
# This script migrates Week 5 audit events from the old format (events.jsonl) to the new format (event_store.jsonl) used in Week 7.

import json
import os
import uuid
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

# ------------- Path setup -------------
# Get the script's current directory
current_script_path = Path(__file__).resolve()

# Go up levels to get to the project root
PROJECT_ROOT = current_script_path.parents[5]

print(f"Project root directory: {PROJECT_ROOT}")

SEED_FILE = (
    PROJECT_ROOT
    / "week5"
    / "Agentic-Event-Store-Enterprise-Audit-Infrastructure"
    / "data"
    / "seed_events.jsonl"
)
OUTPUT_DIR = Path("outputs/week5")

DB_CONFIG = {
    "dbname": "ledger_app",
    "user": "postgres",
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "localhost",
    "port": 5432,
}


def load_seed_events():

    with open(SEED_FILE) as f:
        return [json.loads(line) for line in f if line.strip()]


def convert_seed_event(seed_event, seq_num):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": seed_event.get("event_type"),
        "aggregate_id": seed_event.get("stream_id"),
        "aggregate_type": seed_event.get("event_type", "unknown"),
        "sequence_number": seq_num,
        "payload": seed_event.get("payload", {}),
        "metadata": {"event_version": seed_event.get("event_version", 1)},
        "schema_version": 1,
        "occurred_at": seed_event.get("payload", {}).get("uploaded_at"),
        "recorded_at": seed_event.get("recorded_at"),
    }


def connect_db():
    return psycopg2.connect(**DB_CONFIG)


# --- Event Store Tables ---
def export_events(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT event_id, stream_id, stream_position, event_type, event_version,
               payload, metadata, recorded_at
        FROM events
        ORDER BY global_position ASC
    """)
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "event_id": str(r[0]),
            "event_type": r[3],
            "aggregate_id": r[1],
            "aggregate_type": r[3],
            "sequence_number": r[2],
            "payload": r[5],
            "metadata": r[6],
            "schema_version": r[4],
            "occurred_at": r[5].get("occurred_at") if isinstance(r[5], dict) else None,
            "recorded_at": str(r[7]),
        }
        for r in rows
    ]


def export_event_streams(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT stream_id, aggregate_type, current_version, created_at, archived_at, metadata FROM event_streams"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "stream_id": r[0],
            "aggregate_type": r[1],
            "current_version": r[2],
            "created_at": str(r[3]),
            "archived_at": str(r[4]) if r[4] else None,
            "metadata": r[5],
        }
        for r in rows
    ]


def export_outbox(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT id, event_id, destination, payload, created_at, published_at, attempts, status FROM outbox"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "id": str(r[0]),
            "event_id": str(r[1]),
            "destination": r[2],
            "payload": r[3],
            "created_at": str(r[4]),
            "published_at": str(r[5]) if r[5] else None,
            "attempts": r[6],
            "status": r[7],
        }
        for r in rows
    ]


def export_projection_checkpoints(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT checkpoint_id, projection_name, stream_id, last_position, updated_at, projection_version, checkpoint_metadata FROM projection_checkpoints"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "checkpoint_id": str(r[0]),
            "projection_name": r[1],
            "stream_id": r[2],
            "last_position": r[3],
            "updated_at": str(r[4]),
            "projection_version": r[5],
            "checkpoint_metadata": r[6],
        }
        for r in rows
    ]


# --- Projection Tables ---
def export_application_summary(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT application_id, applicant_id, status, amount, submitted_at, decided_at FROM application_summary"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "application_id": r[0],
            "applicant_id": r[1],
            "status": r[2],
            "amount": float(r[3]) if r[3] is not None else None,
            "submitted_at": str(r[4]) if r[4] else None,
            "decided_at": str(r[5]) if r[5] else None,
        }
        for r in rows
    ]


def export_agent_performance(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT agent_id, model_version, sessions, decisions FROM agent_performance"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {"agent_id": r[0], "model_version": r[1], "sessions": r[2], "decisions": r[3]}
        for r in rows
    ]


def export_compliance_audit(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT compliance_id, event_type, payload, recorded_at FROM compliance_audit"
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "compliance_id": r[0],
            "event_type": r[1],
            "payload": r[2],
            "recorded_at": str(r[3]),
        }
        for r in rows
    ]


def write_jsonl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def run():
    # Seed events
    seed_events = load_seed_events()
    seed_records = [convert_seed_event(ev, i + 1) for i, ev in enumerate(seed_events)]
    write_jsonl(seed_records, OUTPUT_DIR / "seed_events.jsonl")

    # DB exports
    conn = connect_db()
    events = export_events(conn)
    event_streams = export_event_streams(conn)
    outbox = export_outbox(conn)
    checkpoints = export_projection_checkpoints(conn)
    app_summary = export_application_summary(conn)
    agent_perf = export_agent_performance(conn)
    compliance_audit = export_compliance_audit(conn)
    conn.close()

    # Write outputs
    write_jsonl(events, OUTPUT_DIR / "events.jsonl")
    write_jsonl(event_streams, OUTPUT_DIR / "event_streams.jsonl")
    write_jsonl(outbox, OUTPUT_DIR / "outbox.jsonl")
    write_jsonl(checkpoints, OUTPUT_DIR / "projection_checkpoints.jsonl")
    write_jsonl(app_summary, OUTPUT_DIR / "application_summary.jsonl")
    write_jsonl(agent_perf, OUTPUT_DIR / "agent_performance.jsonl")
    write_jsonl(compliance_audit, OUTPUT_DIR / "compliance_audit.jsonl")

    print(
        f"[DONE] Exported: {len(seed_records)} seed events, {len(events)} events, "
        f"{len(event_streams)} streams, {len(outbox)} outbox entries, "
        f"{len(checkpoints)} checkpoints, {len(app_summary)} applications, "
        f"{len(agent_perf)} agent performance rows, {len(compliance_audit)} compliance audits."
    )


if __name__ == "__main__":
    run()
