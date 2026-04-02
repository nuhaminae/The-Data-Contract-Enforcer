# outputs/migrate/week2/migrate_verdicts.py
# This script migrates Week 2 verdict records from the old format (report_onpeer_generated.md+report_onself_generated.md) to the new format (verdict_records.jsonl) used in Week 7.

import json
import re
import statistics
import uuid
from datetime import datetime, timezone
from pathlib import Path

# ------------- Path setup -------------

# Get the script's current directory
current_script_path = Path(__file__).resolve()

# Go up levels to get to the project root
PROJECT_ROOT = current_script_path.parents[5]

print(f"Project root directory: {PROJECT_ROOT}")


# Helper: normalise "8 out of 10" -> 8
def normalise_score(score_label):
    match = re.match(r"(\d+)", score_label)
    return int(match.group(1)) if match else None


# Confidence based on variance of judge scores
def compute_confidence(scores):
    numeric_scores = [s for s in scores if s is not None]
    if not numeric_scores:
        return 0.7
    if len(numeric_scores) == 1:
        return 0.9
    var = statistics.pvariance(numeric_scores)
    conf = 1.0 - (var / 25.0)
    return max(0.5, min(1.0, conf))


# Load rubric.json to get rubric version
with open(
    PROJECT_ROOT / "week2" / "Automation-Auditor" / "rubrics" / "rubric.json"
) as f:
    rubric_data = json.load(f)
rubric_version = rubric_data["rubric_metadata"]["version"]

# Load audit.json
with open(PROJECT_ROOT / "week2" / "Automation-Auditor" / "audit" / "audit.json") as f:
    audit_data = json.load(f)


def build_verdicts_from_json(audit):
    verdicts = []
    for crit in audit["criteria"]:
        scores = []
        numeric_scores = []
        for opinion in crit["judge_opinions"]:
            score = normalise_score(opinion["score_label"])
            numeric_scores.append(score)
            scores.append(
                {
                    "judge": opinion["judge"],
                    "criterion_id": crit["dimension_id"],
                    "score": score,
                    "argument": opinion["argument"],
                    "cited_evidence": opinion.get("cited_evidence", []),
                }
            )
        verdicts.append(
            {
                "verdict_id": str(uuid.uuid4()),
                "target_ref": audit["repo_url"],
                "rubric_id": crit["dimension_id"],
                "rubric_version": rubric_version,
                "scores": scores,
                "overall_verdict": audit["executive_summary"],
                "overall_score": audit["overall_score"],
                "confidence": compute_confidence(numeric_scores),
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return verdicts


# Parse markdown reports
def parse_markdown_report(path):
    with open(path) as f:
        text = f.read()
    repo_match = re.search(r"Audit Report for (https://[^\s]+)", text)
    repo_url = repo_match.group(1) if repo_match else "unknown"
    score_match = re.search(r"\*\*Overall Score:\*\* ([0-9.]+)", text)
    overall_score = float(score_match.group(1)) if score_match else None

    criteria_blocks = re.split(r"## Criterion:", text)[1:]
    verdicts = []
    for block in criteria_blocks:
        lines = block.strip().splitlines()
        crit_line = lines[0]
        crit_id = crit_line.split("(")[-1].strip(")")
        scores = []
        numeric_scores = []
        for line in lines:
            if "Score" in line and "Argument" in line:
                judge = line.split(":")[0].strip("- *")
                score_match = re.search(r"Score (\d+) out of 10", line)
                score = int(score_match.group(1)) if score_match else None
                if score is not None:
                    numeric_scores.append(score)
                arg = line.split("Argument:")[-1].strip()
                scores.append(
                    {
                        "judge": judge,
                        "criterion_id": crit_id,
                        "score": score,
                        "argument": arg,
                        "cited_evidence": [],
                    }
                )
        verdicts.append(
            {
                "verdict_id": str(uuid.uuid4()),
                "target_ref": repo_url,
                "rubric_id": crit_id,
                "rubric_version": rubric_version,
                "scores": scores,
                "overall_verdict": "Automated audit completed.",
                "overall_score": overall_score,
                "confidence": compute_confidence(numeric_scores),
                "evaluated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return verdicts


# Build verdicts from all sources
verdicts = []
verdicts.extend(build_verdicts_from_json(audit_data))
verdicts.extend(
    parse_markdown_report(
        PROJECT_ROOT
        / "week2"
        / "Automation-Auditor"
        / "audit"
        / "report_onpeer_generated.md"
    )
)
verdicts.extend(
    parse_markdown_report(
        PROJECT_ROOT
        / "week2"
        / "Automation-Auditor"
        / "audit"
        / "report_onself_generated.md"
    )
)

# Write JSONL
with open("outputs/week2/verdicts.jsonl", "w") as f:
    for v in verdicts:
        f.write(json.dumps(v) + "\n")

print(f"Wrote {len(verdicts)} verdict records to outputs/week2/verdicts.jsonl")
