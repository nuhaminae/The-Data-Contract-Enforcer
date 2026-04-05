# outputs/migrate/week4/migrate_lineage_snapshots.py
# This script migrates Week 4 lineage snapshots from module_graph.json into week 7 format (lineage_snapshots.jsonl).

import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests  # To communicate with the local Ollama server

# --- Ollama and Path Setup ---

# The default endpoint for a local Ollama server.
OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
MODEL_NAME = "Qwen2.5:3B"  # The specific model that can be used.

# Path setup
try:
    current_script_path = Path(__file__).resolve()
    PROJECT_ROOT = current_script_path.parents[5]
except NameError:
    PROJECT_ROOT = Path.cwd()

print(f"Project root directory: {PROJECT_ROOT}")

INPUT_MODULE_GRAPH = (
    PROJECT_ROOT
    / "week4"
    / "The-Brownfield-Cartographer"
    / ".cartography"
    / "module_graph.json"
)

OUTPUT_FILE = (
    PROJECT_ROOT
    / "week7"
    / "The-Data-Contract-Enforcer"
    / "outputs"
    / "week4"
    / "lineage_snapshots.jsonl"
)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

print(f"Input module graph: {INPUT_MODULE_GRAPH}")
print(f"Output file: {OUTPUT_FILE}")
print(f"Using local Ollama model: {MODEL_NAME}")


# --- Helper Functions ---
def get_language_from_extension(path_str):
    extension = "".join(Path(path_str).suffixes)
    lang_map = {
        ".py": "python",
        ".pyi": "python",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".md": "markdown",
        ".js": "javascript",
        ".ts": "typescript",
        ".json": "json",
        ".toml": "toml",
        ".lock": "text",
        ".cfg": "ini",
    }
    return lang_map.get(extension, "text")


def normalise_path(path_str):
    return path_str.replace("\\\\", "/").replace("\\", "/")


def get_llm_inferred_purpose(file_path, code_content):
    return "Purpose inference skipped in this environment. (LLM calls are disabled.)"


'''
# --- NEW: Local LLM Function (Ollama) ---
def get_llm_inferred_purpose(file_path, code_content):
    """
    Sends file content to the local Ollama API to get a one-sentence purpose summary.
    """
    if not code_content or code_content.isspace():
        return "Not applicable (e.g., empty or binary file)."

    print(f"   > Inferring purpose for {file_path} using Ollama...")

    prompt = f"""
    Analyse the following code from the file '{file_path}'. Based on its content, provide a concise, one-sentence summary of its primary purpose.

    CODE:
    ---
    {code_content}
    ---
    
    ONE-SENTENCE SUMMARY:
    """

    try:
        # Construct the payload for the Ollama API
        payload = {
            "model": MODEL_NAME,
            "prompt": prompt,
            "stream": False,  # We want the full response at once
        }

        # Send the request to the local server
        response = requests.post(OLLAMA_ENDPOINT, json=payload)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

        # The actual text response is inside the 'response' key of the JSON
        summary = (
            response.json().get("response", "Failed to get summary from model.").strip()
        )

        # Add a small delay to be kind to your local machine
        time.sleep(0.5)
        return summary
    except requests.exceptions.ConnectionError:
        # This error happens if the Ollama server isn't running.
        print("      - LLM Error: Could not connect to Ollama server. Is it running?")
        return "Purpose inference failed: Ollama server not reachable."
    except Exception as e:
        print(f"      - LLM Error for {file_path}: {e}")
        return "Purpose inference failed due to an API error."
'''


# --- Main Processing Logic ---
def process_module_graph(graph_data):
    source_nodes = graph_data.get("nodes", [])
    if not source_nodes:
        return None

    first_path_raw = source_nodes[0].get("path", "")
    first_path_normalised = normalise_path(first_path_raw)
    root_dir_name = Path(first_path_normalised).parts[0]
    codebase_root = f"https://github.com/{root_dir_name}"
    print(f"Dynamically determined codebase_root: {codebase_root}")

    canonical_nodes = []
    normalised_path_to_canonical_id = {}

    # Small sample for testing purposes. Remove '[:5]' to run on all files.
    # for node_data in source_nodes[:5]:
    for node_data in source_nodes:
        raw_path = node_data.get("path")
        if not raw_path:
            continue

        normalised_path = normalise_path(raw_path)
        relative_path = (
            normalised_path[len(root_dir_name) + 1 :]
            if normalised_path.startswith(root_dir_name + "/")
            else normalised_path
        )

        canonical_id = f"file::{relative_path}"
        normalised_path_to_canonical_id[normalised_path] = canonical_id

        # Call the LLM for code files
        code = node_data.get("attrs", {}).get("code")
        language = get_language_from_extension(relative_path)

        # Only infer purpose for actual code files to save time
        if language == "python":
            purpose = get_llm_inferred_purpose(relative_path, code)
        else:
            purpose = f"A {language.upper()} configuration or data file."

        canonical_node = {
            "node_id": canonical_id,
            "type": "FILE",
            "label": Path(relative_path).name,
            "metadata": {
                "path": relative_path,
                "language": language,
                "purpose": purpose,
                "last_modified": datetime.now(timezone.utc).isoformat(),
            },
        }
        canonical_nodes.append(canonical_node)

    # The edge processing logic remains the same and should now work
    canonical_edges = []
    for edge_data in graph_data.get("edges", []):
        source_key_normalised = normalise_path(edge_data.get("source", ""))
        target_key_raw = edge_data.get("target", "")

        if source_key_normalised in normalised_path_to_canonical_id:
            target_id = normalised_path_to_canonical_id.get(
                normalise_path(target_key_raw), target_key_raw
            )
            edge = {
                "source": normalised_path_to_canonical_id[source_key_normalised],
                "target": target_id,
                "relationship": edge_data.get("type", "UNKNOWN").upper(),
                "confidence": 0.90,
            }
            canonical_edges.append(edge)

    snapshot = {
        "snapshot_id": str(uuid.uuid4()),
        "codebase_root": codebase_root,
        "git_commit": "d9f225d7c8e6a4b1f3a5e7d9c8b0a6e1d4f3c2b1",
        "nodes": canonical_nodes,
        "edges": canonical_edges,
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    return snapshot


# --- Main Execution ---
if not INPUT_MODULE_GRAPH.exists():
    print(f"Error: Input file not found at {INPUT_MODULE_GRAPH}")
else:
    with open(INPUT_MODULE_GRAPH, "r", encoding="utf-8") as f:
        module_graph = json.load(f)

    lineage_snapshot = process_module_graph(module_graph)

    if lineage_snapshot:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(json.dumps(lineage_snapshot, indent=None) + "\n")

        print(
            f"\n✅ Success! Created lineage snapshot with {len(lineage_snapshot['nodes'])} nodes and {len(lineage_snapshot['edges'])} edges."
        )
        print(f"Output written to: {OUTPUT_FILE}")
