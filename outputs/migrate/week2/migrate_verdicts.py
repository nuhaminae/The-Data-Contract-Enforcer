# outputs/migrate/week2/migrate_verdicts.py
# This script migrates Week 2 verdict records from the old format (report_onpeer_generated.md+report_onself_generated.md) to the new format (verdict_record.jsonl) used in Week 7.

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ------------- Path setup -------------
# This setup restores the original pathing from the script.
# It assumes the script is located in a specific directory structure relative to the project root.
try:
    current_script_path = Path(__file__).resolve()
    # Path to the root of the 'The-Data-Contract-Enforcer' project.
    PROJECT_ROOT = current_script_path.parents[3]

    # Path to the root of the 'student-work' directory.
    CODE_PROJECT_DIRECTORY = current_script_path.parents[5]
    # The specific root of the Week 2 'Automation-Auditor' project that contains the audit folder.
    CODE_PROJECT_ROOT = (
        CODE_PROJECT_DIRECTORY / "week2" / "Automation-Auditor" / "audit"
    )
except NameError:
    # Fallback for interactive environments where __file__ is not defined
    print(
        "Warning: '__file__' not defined. Using current working directory as a fallback for paths."
    )
    PROJECT_ROOT = Path.cwd()
    CODE_PROJECT_ROOT = PROJECT_ROOT

print(f"Project root determined as: {PROJECT_ROOT}")
print(f"Code project root for audit files determined as: {CODE_PROJECT_ROOT}")

# Define and create the output directory
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "week2"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print(f"Output directory is set to: {OUTPUT_DIR}")


def get_sha256(text):
    """Computes the SHA-256 hash of a given text string."""
    return hashlib.sha256(text.encode()).hexdigest()


def normalise_to_5(score_10):
    """Normalises a score from a 0-10 scale to a 1-5 scale."""
    if score_10 is None:
        return 3
    return round(float(score_10) / 2)


def map_overall_verdict(score_5):
    """Maps a 1-5 score to a canonical verdict string (PASS, FAIL, WARN)."""
    if score_5 >= 4.0:
        return "PASS"
    if score_5 < 3.0:
        return "FAIL"
    return "WARN"


def parse_verdicts_from_markdown(file_path):
    """
    Parses a markdown audit report to extract verdict details and structure them
    into the canonical format.
    """
    with open(file_path, "r") as f:
        content = f.read()

    # 1. Extract target repository URL from the main header
    target_match = re.search(r"Audit Report for (https://[^\s\n]+)", content)
    target_ref = target_match.group(1) if target_match else "unknown_target"

    # 2. Extract the overall numerical score
    score_match = re.search(r"\*\*Overall Score:\*\*\s+([0-9.]+)", content)
    overall_score = float(score_match.group(1)) if score_match else 3.0

    # 3. Parse each criterion section to populate the 'scores' dictionary
    # Splits the document by '## Criterion: ' which is the correct header level
    sections = re.split(r"## Criterion: ", content)[1:]
    scores_dict = {}

    for sec in sections:
        # Extract criterion name and ID from the section header
        header_match = re.search(r"^(.*?)\s+\((.*?)\)", sec)
        if not header_match:
            continue
        crit_name, crit_id = header_match.groups()

        # Regex to find judge opinions starting with '-' and capture judge, score, and argument
        judge_pattern = r"-\s+\*\*(.*?)\*\*: Score (\d+) out of 10, Argument: (.*?)(?=\n-|\n##|# Remediation|$)"
        opinions = re.findall(judge_pattern, sec, re.DOTALL)

        if opinions:
            # Normalise scores from 1-10 to 1-5 and calculate the average
            norm_scores = [normalise_to_5(int(o[1])) for o in opinions]
            avg_crit_score = sum(norm_scores) / len(norm_scores)

            # Join all judge arguments into a single 'notes' string
            notes = " | ".join([f"{o[0]}: {o[2].strip()}" for o in opinions])

            # Assemble the dictionary for the current criterion
            scores_dict[crit_id] = {
                "score": int(round(avg_crit_score)),
                "evidence": [],  # This can be extended to extract evidence if available
                "notes": notes,
            }

    # 4. Construct the final canonical record for the verdict
    return {
        "verdict_id": str(uuid.uuid4()),
        "target_ref": target_ref,
        "rubric_id": get_sha256("automation_auditor_v3_rubric"),
        "rubric_version": "3.0.0",
        "scores": scores_dict,
        "overall_verdict": map_overall_verdict(overall_score),
        "overall_score": round(overall_score, 2),
        "confidence": 0.95,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }


# --- Main Execution Block ---
# Define the source markdown files using the corrected code project root path
source_files = [
    CODE_PROJECT_ROOT / "report_onpeer_generated.md",
    CODE_PROJECT_ROOT / "report_onself_generated.md",
]

print(f"Source files to be processed: {[str(f) for f in source_files]}")

verdicts = []
for f_path in source_files:
    if f_path.exists():
        print(f"Processing {f_path.name}...")
        verdicts.append(parse_verdicts_from_markdown(f_path))
    else:
        print(f"Warning: Source file not found - {f_path}")

# Write the processed verdicts to the output JSONL file
output_file = OUTPUT_DIR / "verdict_record.jsonl"
with open(output_file, "w") as f:
    for v in verdicts:
        f.write(json.dumps(v) + "\n")

if verdicts:
    print(f"\nSuccessfully migrated {len(verdicts)} canonical records to {output_file}")
else:
    print(
        "\nNo verdict records were generated. Please check if source files exist and paths are correct."
    )
