# contracts/generator.py
# This script generates a Bitol-compliant data contract and a corresponding dbt model file.

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import yaml


def profile_data(df: pd.DataFrame) -> dict:
    """
    Analyses a pandas DataFrame and generates a Bitol-compliant schema object.
    The output is a dictionary where keys are column names.
    """
    profiles = {}
    for col in df.columns:
        dtype = df[col].dtype

        if np.issubdtype(dtype, np.integer):
            col_type = "integer"
        elif np.issubdtype(dtype, np.floating):
            col_type = "float"
        elif np.issubdtype(dtype, np.datetime64):
            col_type = "timestamp"
        elif dtype == "bool":
            col_type = "boolean"
        else:
            col_type = "string"

        # The schema for each column is now a dictionary.
        profiles[col] = {
            "type": col_type,
            # Bitol uses 'required', which is the inverse of 'nullable'.
            "required": not bool(df[col].isnull().any()),
            "description": f"Inferred {col_type} column.",
        }
    return profiles


class ContractGenerator:
    """
    Generates a Bitol data contract and a dbt model file from a data source.
    """

    def __init__(
        self,
        source: str,
        contract_id: str,
        lineage: str | None = None,
        output: str | None = None,
    ):
        self.source = source
        self.contract_id = contract_id
        self.lineage = lineage
        self.output = output or "generated_contracts/"
        self.source_name = Path(source).stem
        self.output_path = Path(self.output)
        self.output_path.mkdir(parents=True, exist_ok=True)

    def load_and_flatten(self) -> pd.DataFrame:
        """
        Load and flatten the source JSONL data.
        """
        with open(self.source, "r", encoding="utf-8") as f:
            records = [json.loads(l) for l in f if l.strip()]
        return pd.json_normalize(records, sep="_")

    def load_lineage_graph(self) -> dict:
        """
        Load the lineage graph from the specified file.
        """
        if self.lineage:
            with open(self.lineage, "r", encoding="utf-8") as f:
                return json.load(f)
        return None

    def get_upstream_sources(self, lineage_graph: dict) -> list[str]:
        """
        Finds upstream sources from the lineage graph related to the contract ID.
        """
        if not lineage_graph:
            return []

        source_systems = set()
        edges = lineage_graph.get("edges", [])

        for edge in edges:
            # Check if any part of the contract ID (e.g., 'week1', 'intent') is in the edge source
            if any(
                part in edge.get("source", "") for part in self.contract_id.split("-")
            ):
                source_systems.add(edge["source"])

        return list(source_systems)

    def build_bitol_contract(self, schema: dict, upstream_sources: list) -> dict:
        """
        Builds the main data contract in the Bitol-compliant format.
        """
        return {
            "kind": "DataContract",
            "apiVersion": "v3.0.0",
            "id": self.contract_id,
            "info": {
                "title": f"Auto-generated Contract: {self.contract_id}",
                "version": "1.0.0",
                "description": "Baseline contract generated from production data snapshots.",
            },
            "schema": schema,
            "quality": {
                "type": "SodaChecks",
                "specification": {"checks": []},  # Placeholder for data quality checks
            },
            "lineage": {"upstream": upstream_sources, "downstream": []},
        }

    def build_dbt_model(self, schema: dict) -> dict:
        """
        Builds the dbt model file with 'not_null' tests for required columns.
        """
        columns = []
        for name, details in schema.items():
            col_spec = {"name": name}
            tests = []
            if details.get("required"):
                tests.append("not_null")
            if tests:
                col_spec["tests"] = tests
            columns.append(col_spec)

        return {
            "version": 2,
            "models": [{"name": self.contract_id, "columns": columns}],
        }

    def run(self):
        """
        Run the data contract generator and save both files.
        """
        df = self.load_and_flatten()
        schema = profile_data(df)

        lineage_graph = self.load_lineage_graph()
        upstream_sources = self.get_upstream_sources(lineage_graph)

        # Build and save the main Bitol contract
        bitol_contract = self.build_bitol_contract(schema, upstream_sources)
        bitol_output_path = self.output_path / f"{self.contract_id}.yaml"
        with open(bitol_output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                bitol_contract,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        print(f"✅ Successfully created Bitol contract: {bitol_output_path}")

        # Build and save the dbt model file
        dbt_model = self.build_dbt_model(schema)
        dbt_output_path = self.output_path / f"{self.contract_id}_dbt.yml"
        with open(dbt_output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                dbt_model,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        print(f"✅ Successfully created dbt model: {dbt_output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate a Bitol-compliant data contract and dbt model."
    )
    parser.add_argument("--source", help="Path to the data source file.", required=True)
    parser.add_argument(
        "--contract-id", help="The ID of the data contract.", required=True
    )
    parser.add_argument(
        "--lineage", help="Path to the lineage graph file.", required=False
    )
    parser.add_argument(
        "--output", help="Path to the output directory.", required=False
    )

    args = parser.parse_args()

    gen = ContractGenerator(
        source=args.source,
        contract_id=args.contract_id,
        lineage=args.lineage,
        output=args.output,
    )
    gen.run()
