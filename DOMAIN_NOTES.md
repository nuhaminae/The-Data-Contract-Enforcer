# DOMAIN NOTES.md

## 1. Backward-Compatible vs. Breaking Schema Changes

Backward-compatible schema changes are those that do not break existing functionality or require changes to existing code. Where as breaking schema changes are those that require changes to exisitng code or break exisitng functionality.
  
| Weeks | Backward-Compatible Changes Examples | Breaking Changes Examples |
| --- | --- | --- |
| Week 1 (Intent-Code Correlator) | Adding an optional `intent_category` field to classify intents. Existing consumers still parse records. | |
| Week 2 (Digital Courtroom) | Introducing a new rubric criterion `documentation_quality` as nullable. Verdicts remain valid. | Removing `overall_verdict`. Consumers relying on PASS/FAIL/WARN logic fail. |
| Week 3 (Document Refinery) | | Changing `confidence` from float 0.0–1.0 to integer 0–100. Downstream consumers misinterpret values. |
| Week 4 (Brownfield Cartographer) | Adding a new node type `EXTERNAL` in the lineage graph. Consumers can ignore unknown types. | |
| Week 5 (Event Sourcing Platform) | | Renaming `aggregate_id` to `agg_id`. Breaks consumers expecting the old field. |

---

## 2. Week 3 Confidence Field Change

**Scenario:**  

- Week 3 extractor outputs `confidence` as float 0.0–1.0.  
- Update changes it to integer 0–100.  
- Week 4 Cartographer ingests facts into lineage metadata.  
- Downstream consumers misinterpret values (e.g., 87 → 87.0 instead of 0.87).  
- Silent corruption propagates into lineage graph and blast radius reports.

**Contract Clause (Bitol YAML):**

```yaml
confidence:
  type: number
  minimum: 0.0
  maximum: 1.0
  required: true
  description: Confidence score must remain normalised float in [0.0, 1.0].
```

This clause enforces the normalised float range and catches the breaking change before propagation.

---

## 3. Lineage Graph & Blame Chain

**Step-by-Step Traversal Logic:**

1. **Violation detected** (e.g., confidence out of range).  
2. **Load Week 4 lineage graph.**  
3. **Locate failing schema element** (`extracted_facts.confidence`).  
4. **Breadth-first traversal upstream:**  
   - Find node producing the field (e.g., `src/week3/extractor.py`).  
   - Stop at external boundary or repo root.  
5. **Git blame integration:**  
   - Run `git log --follow` for file commits.  
   - Run `git blame` for line-level attribution.  
6. **Blame chain output:**  
   - Commit hash, author, timestamp, commit message.  
   - Confidence score adjusted by lineage hops.  
7. **Blast radius report:**  
   - List downstream nodes affected.  
   - Estimate number of failing records.

---

## 4. LangSmith Trace Record Contract (Bitol YAML)

```yaml
kind: DataContract
apiVersion: v3.0.0
id: langsmith-trace-record
info:
  title: LangSmith Trace Records
  version: 1.0.0
  owner: week7-team
schema:
  id:
    type: string
    format: uuid
    required: true
  run_type:
    type: string
    enum: [llm, chain, tool, retriever, embedding]
    required: true
  total_tokens:
    type: integer
    minimum: 0
    required: true
    statistical:
      rule: total_tokens = prompt_tokens + completion_tokens
  total_cost:
    type: number
    minimum: 0.0
    required: true
  start_time:
    type: string
    format: date-time
  end_time:
    type: string
    format: date-time
    constraint: end_time > start_time
ai_extensions:
  - clause: "Embedding drift detection on outputs"
  - clause: "Structured output schema enforcement"
```

- **Structural clause:** `id` must be UUID.  
- **Statistical clause:** `total_tokens = prompt_tokens + completion_tokens`.  
- **AI-specific clause:** Embedding drift detection.

---

## 5. Common Failure Mode of Contract Enforcement

**Failure Mode:**  
Contracts get stale when upstream producers evolve schemas without updating contracts.  

- Example: Week 3 extractor changes confidence scale but contract remains unchanged.  
- Downstream consumers silently break.

**Why Contracts Get Stale:**  

- Lack of ownership.  
- No automated regeneration.  
- Contracts treated as documentation, not executable promises.

**Architecture Prevention:**  

- **Automated ContractGenerator:** regenerates contracts on every run.  
- **Schema Snapshots:** stored in `schema_snapshots/` for drift detection.  
- **ValidationRunner:** catches violations in real time.  
- **ViolationAttributor:** traces failures to commits, forcing accountability.  
- **AI Extensions:** monitor embedding drift and LLM output schema violations.

---

## Reflection

Writing contracts revealed hidden assumptions:  

- Confidence values assumed normalised but not enforced.  
- Verdict scores assumed valid integers but lacked explicit checks.  
- Event sequence numbers assumed monotonic but not validated.  

Contracts transform these assumptions into enforceable promises, preventing silent corruption and enabling traceable blame chains.
