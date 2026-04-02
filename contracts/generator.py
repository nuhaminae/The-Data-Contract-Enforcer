# contracts/generator.py
# Builds JSON/YAML contracts from extracted data profiles.
# RUN Example : python contracts/generator.py --source outputs/week1/intent_records.jsonl --contract-id week1-intent-records --lineage outputs/week4/lineage_snapshots.jsonl --output generated_contracts/

import json
import pandas as pd
import yaml
import argparse
import shutil
from pathlib import Path
from datetime import datetime, timezone

class ContractGenerator:
    def __init__(self, source_path, contract_id, lineage_path, output_dir):
        self.source_path = Path(source_path)
        self.contract_id = contract_id
        self.lineage_path = Path(lineage_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_and_flatten(self):
        """Stage 1: Load JSONL and flatten nested structures (e.g., Week 3 facts) [5]."""
        with open(self.source_path) as f:
            records = [json.loads(l) for l in f if l.strip()]
        
        rows = []
        for r in records:
            # Separate top-level fields from nested arrays
            base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
            # Explode extracted_facts for Week 3 profiling [5]
            facts = r.get('extracted_facts', [{}])
            for fact in facts:
                # Prefix nested fields to avoid collisions [7]
                rows.append({**base, **{f'fact_{k}': v for k, v in fact.items()}})
        return pd.DataFrame(rows)

    def profile_columns(self, df):
        """Stage 2: Structural and Statistical Profiling [8, 9]."""
        profiles = {}
        for col in df.columns:
            series = df[col]
            profile = {
                'name': col,
                'dtype': str(series.dtype),
                'null_fraction': float(series.isna().mean()),
                'cardinality_estimate': int(series.nunique()),
                'sample_values': [str(v) for v in series.dropna().unique()[:5]],
            }
            # Mandatory numeric stats for statistical drift detection [8, 10]
            if pd.api.types.is_numeric_dtype(series):
                profile['stats'] = {
                    'min': float(series.min()), 'max': float(series.max()),
                    'mean': float(series.mean()), 'stddev': float(series.std()),
                    'p25': float(series.quantile(0.25)), 'p50': float(series.quantile(0.5)),
                    'p95': float(series.quantile(0.95)), 'p99': float(series.quantile(0.99))
                }
            profiles[col] = profile
        return profiles

    def build_bitol_contract(self, profiles, df):
        """Stage 3 & 4: Translate to Bitol YAML and Inject Lineage [11-13]."""
        # Metadata [11]
        contract = {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": self.contract_id,
            "info": {
                "title": f"Auto-generated Contract: {self.contract_id}",
                "version": "1.0.0",
                "description": "Baseline contract generated from production data snapshots."
            },
            "schema": {},
            "quality": {"type": "SodaChecks", "specification": {"checks": []}},
            "lineage": {"upstream": [], "downstream": []}
        }

        # Generate Clauses [12]
        for col, profile in profiles.items():
            dtype_map = {'float64': 'number', 'int64': 'integer', 'bool': 'boolean', 'object': 'string'}
            bitol_type = dtype_map.get(profile['dtype'], 'string')
            
            clause = {
                "type": bitol_type,
                "required": profile['null_fraction'] == 0.0,
                "description": f"Inferred {bitol_type} column with cardinality {profile['cardinality_estimate']}"
            }

            # Week-specific logic for confidence/IDs/Dates [12, 14, 15]
            if 'confidence' in col:
                clause.update({"minimum": 0.0, "maximum": 1.0})
            if col.endswith('_id'):
                clause.update({"format": "uuid", "pattern": "^[0-9a-f-]{36}$"})
            if col.endswith('_at'):
                clause.update({"format": "date-time"})
            
            contract["schema"][col] = clause

        # Inject Lineage [13]
        if self.lineage_path.exists():
            with open(self.lineage_path) as f:
                snapshot = json.loads(f.readlines()[-1]) # Latest snapshot [13]
            consumers = [e['target'] for e in snapshot.get('edges', []) 
                         if self.contract_id.split('-') in e['source']]
            contract['lineage']['downstream'] = [{"id": c, "fields_consumed": list(df.columns[:3])} for c in consumers]

        return contract

    def generate_dbt_output(self, contract):
        """Stage 5: Parallel dbt schema.yml with test definitions [4]."""
        dbt_schema = {"version": 2, "models": [{"name": self.contract_id, "columns": []}]}
        for col, clause in contract["schema"].items():
            tests = []
            if clause.get("required"): tests.append("not_null")
            if clause.get("unique"): tests.append("unique")
            dbt_schema["models"][0]["columns"].append({"name": col, "tests": tests})
        return dbt_schema

    def run(self):
        df = self.load_and_flatten()
        profiles = self.profile_columns(df)
        contract = self.build_bitol_contract(profiles, df)
        dbt_yaml = self.generate_dbt_output(contract)

        # Write Main Contract
        main_path = self.output_dir / f"{self.contract_id}.yaml"
        with open(main_path, 'w') as f:
            yaml.dump(contract, f, sort_keys=False)

        # Write dbt Version [4]
        with open(self.output_dir / f"{self.contract_id}_dbt.yml", 'w') as f:
            yaml.dump(dbt_yaml, f, sort_keys=False)

        # Snapshot Discipline: Mandatory for Phase 3 [6, 16]
        snap_dir = Path("schema_snapshots") / self.contract_id
        snap_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        shutil.copy(main_path, snap_dir / f"{ts}.yaml")
        print(f" Generated contract and snapshot for {self.contract_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--contract-id", required=True)
    parser.add_argument("--lineage", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    gen = ContractGenerator(args.source, args.contract_id, args.lineage, args.output)
    gen.run()