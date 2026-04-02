# outputs/migrate/week4/migrate_lineage_snapshots.py
# This script migrates Week 4 lineage snapshots by merging the lineage_graph.json and module_graph.json
# into a single JSONL file that preserves all fields from both graphs.

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ------------- Path setup -------------
# Get the script's current directory
current_script_path = Path(__file__).resolve()

# Go up levels to get to the project root
PROJECT_ROOT = current_script_path.parents[5]

print(f"Project root directory: {PROJECT_ROOT}")


LINEAGE_FILE = (
    PROJECT_ROOT
    / "week4"
    / "The-Brownfield-Cartographer"
    / ".cartography"
    / "lineage_graph.json"
)
MODULE_FILE = (
    PROJECT_ROOT
    / "week4"
    / "The-Brownfield-Cartographer"
    / ".cartography"
    / "module_graph.json"
)
OUTPUT_FILE = Path("outputs/week4/lineage_snapshots.jsonl")


def load_json(path):
    with open(path) as f:
        return json.load(f)


def summarise_graphs(lineage_graph, module_graph):
    return {
        "lineage_node_count": len(lineage_graph.get("nodes", [])),
        "lineage_edge_count": len(lineage_graph.get("edges", [])),
        "lineage_sources_count": len(lineage_graph.get("sources", [])),
        "lineage_sinks_count": len(lineage_graph.get("sinks", [])),
        "module_node_count": len(module_graph.get("nodes", [])),
        "module_edge_count": len(module_graph.get("edges", [])),
        "pagerank_count": len(module_graph.get("pagerank", {})),
        "velocity_hotspots_count": len(
            module_graph.get("velocity", {}).get("hotspots", [])
        ),
        "dead_functions_count": module_graph.get("velocity", {})
        .get("dead_code_summary", {})
        .get("dead_functions_count", 0),
        "dead_classes_count": module_graph.get("velocity", {})
        .get("dead_code_summary", {})
        .get("dead_classes_count", 0),
        "orphan_modules_count": module_graph.get("velocity", {})
        .get("dead_code_summary", {})
        .get("orphan_modules_count", 0),
    }


def consolidate(lineage_graph, module_graph):
    summary = summarise_graphs(lineage_graph, module_graph)
    record = {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": lineage_graph.get("codebase_root", "meltano"),
        "git_commit": lineage_graph.get("git_commit", "unknown"),
        "lineage_graph": lineage_graph,  # full lineage graph
        "module_graph": module_graph,  # full module graph
        "summary": summary,  # quick metadata counts
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }
    return [record]


def write_jsonl(records, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def run():
    lineage_graph = load_json(LINEAGE_FILE)
    module_graph = load_json(MODULE_FILE)
    records = consolidate(lineage_graph, module_graph)
    write_jsonl(records, OUTPUT_FILE)
    print(f"[DONE] Wrote {len(records)} merged snapshot(s) to {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
