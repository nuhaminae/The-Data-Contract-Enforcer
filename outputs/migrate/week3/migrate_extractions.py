# outputs/migrate/week3/migrate_extractions.py
# This script migrates Week 3 extraction records from the old format (inside .refinery/profiles, .refinery/extracted, and .refinery/pageindex)
# to the new format (extraction_record.jsonl) used in Week 7.


import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ------------- Path setup -------------
try:
    current_script_path = Path(__file__).resolve()
    PROJECT_ROOT = current_script_path.parents[5]
except NameError:
    print(
        "Warning: '__file__' not defined. Using current working directory as project root."
    )
    PROJECT_ROOT = Path.cwd()

print(f"Project root directory: {PROJECT_ROOT}")

PROFILE_DIR = (
    PROJECT_ROOT / "week3" / "Document-Intelligence-Refinery" / ".refinery" / "profiles"
)
EXTRACTED_DIR = (
    PROJECT_ROOT
    / "week3"
    / "Document-Intelligence-Refinery"
    / ".refinery"
    / "extracted"
)
PAGE_INDEX_DIR = (
    PROJECT_ROOT
    / "week3"
    / "Document-Intelligence-Refinery"
    / ".refinery"
    / "pageindex"
)
OUTPUT_FILE = (
    PROJECT_ROOT
    / "week7"
    / "The-Data-Contract-Enforcer"
    / "outputs"
    / "week3"
    / "extraction_record.jsonl"
)
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

print(f"Profiles directory: {PROFILE_DIR}")
print(f"Extractions directory: {EXTRACTED_DIR}")
print(f"Page Index directory: {PAGE_INDEX_DIR}")
print(f"Output file will be written to: {OUTPUT_FILE}")


def get_file_sha256(file_path):
    """Computes the SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def load_json_safely(file_path):
    """Loads a JSON file and handles potential errors gracefully."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Warning: Could not load or parse {file_path}. Error: {e}.")
        return None


def process_files():
    """Builds the canonical extraction_record.jsonl with the corrected entity logic."""
    all_canonical_records = []
    page_index_files = list(PAGE_INDEX_DIR.glob("*_pageindex.json"))
    if not page_index_files:
        print(f"Error: No pageindex files found in {PAGE_INDEX_DIR}. Cannot proceed.")
        return

    prefix_regex = re.compile(
        r"Here is a summary of the provided text:\\n\\n\*\*.*?\*\*\\n", re.DOTALL
    )

    for page_index_file in page_index_files:
        base_name = page_index_file.name.replace("_pageindex.json", "")
        profile_data = load_json_safely(PROFILE_DIR / f"{base_name}.json")
        extracted_data = load_json_safely(EXTRACTED_DIR / f"{base_name}.json")
        page_index_data = load_json_safely(page_index_file)

        if not (profile_data and extracted_data and page_index_data):
            print(f"--- Incomplete file set for {base_name}. Skipping. ---\n")
            continue

        print(f"Processing file set for: {base_name}")

        canonical_facts = []
        canonical_entities = []
        entity_counter = 1  # A single counter for the entire document.

        for page in page_index_data.get("pages", []):
            page_num = page.get("page_number", 1)
            page_summaries, page_excerpts = [], []

            # This list will hold the sequential IDs for the current fact (page).
            current_fact_entity_refs = []

            for section in page.get("sections", []):
                summary = section.get("section_summary", "")
                cleaned_summary = prefix_regex.sub("", summary).strip()
                if cleaned_summary:
                    page_summaries.append(cleaned_summary)
                page_excerpts.append(section.get("content", {}).get("text", ""))

                # For every section that has content, create a new entity.
                section_text = section.get("content", {}).get("text")
                if section_text:
                    human_readable_id = f"entity_id_{entity_counter}"

                    # Add the new sequential ID to this page's list of references.
                    current_fact_entity_refs.append(human_readable_id)

                    # Create the corresponding entity object.
                    entity = {
                        "entity_id": str(uuid.uuid4()),  # A new UUID for the entity.
                        "name": human_readable_id,  # The sequential ID is the name.
                        "type": profile_data.get("domain_hint", "GENERAL"),
                        "canonical_value": section_text,  # The descriptive text.
                    }
                    canonical_entities.append(entity)
                    entity_counter += 1  # Increment the counter for the next entity.

            if page_summaries:
                fact = {
                    "fact_id": str(uuid.uuid4()),
                    "text": "\n\n".join(page_summaries),
                    "entity_refs": current_fact_entity_refs,  # Use the list of sequential IDs.
                    "confidence": extracted_data.get("extraction_confidence", 0.9),
                    "page_ref": page_num,
                    "source_excerpt": "\n\n".join(page_excerpts),
                }
                canonical_facts.append(fact)

        cost_estimate = extracted_data.get("cost_estimate", {})

        canonical_record = {
            "doc_id": str(uuid.uuid4()),
            "source_path": profile_data.get("file_path", "Path not specified"),
            "source_hash": get_file_sha256(page_index_file),
            "extracted_facts": canonical_facts,
            "entities": canonical_entities,
            "extraction_model": extracted_data.get("strategy_used", "default_model"),
            "processing_time_ms": int(cost_estimate.get("runtime_sec", 0) * 1000),
            "token_count": {
                "input": cost_estimate.get("tokens", 0),
                "output": cost_estimate.get("tokens", 0),
            },
            "extracted_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        all_canonical_records.append(canonical_record)
        print(
            f"Successfully processed {base_name}. Created {len(canonical_facts)} page-level facts and {len(canonical_entities)} unique entities."
        )

    if all_canonical_records:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for record in all_canonical_records:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(
            f"\n✅ Success! Created {len(all_canonical_records)} canonical records in:\n{OUTPUT_FILE}"
        )
    else:
        print("\nNo records were generated. Please check input file paths and content.")


if __name__ == "__main__":
    process_files()
