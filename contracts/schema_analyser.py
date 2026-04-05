# contracts/schema_analyser.py
# This script reads a Bitol data contract and generates a summary analysis of its schema.

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import yaml


class SchemaAnalyser:
    """
    Analyses the schema of a Bitol data contract and generates a report.
    """

    def __init__(self, contract_path: str, output_path: str):
        self.contract_path = Path(contract_path)
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.contract = None

    def load_contract(self) -> bool:
        """Loads the YAML data contract, ensuring UTF-8 encoding."""
        print(f"  - Loading contract: {self.contract_path}")
        try:
            with open(self.contract_path, "r", encoding="utf-8") as f:
                self.contract = yaml.safe_load(f)
            return True
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"    - Error loading contract: {e}")
            return False

    def analyse_schema(self) -> dict:
        """
        Performs a detailed analysis of the contract's schema section.
        """
        print("  - Analysing schema...")
        schema = self.contract.get("schema", {})
        if not schema:
            return {
                "status": "error",
                "message": "Contract does not contain a valid 'schema' block.",
            }

        total_columns = len(schema)
        column_types = [details.get("type", "unknown") for details in schema.values()]
        type_counts = dict(Counter(column_types))

        required_columns = [
            name for name, details in schema.items() if details.get("required")
        ]

        undescribed_columns = [
            name for name, details in schema.items() if not details.get("description")
        ]

        analysis = {
            "total_columns": total_columns,
            "column_type_counts": type_counts,
            "required_columns_count": len(required_columns),
            "required_columns": required_columns,
            "undescribed_columns_count": len(undescribed_columns),
            "undescribed_columns": undescribed_columns,
        }

        print(f"    - Found {total_columns} total columns.")
        print(f"    - Found {len(required_columns)} required columns.")
        return analysis

    def write_report(self, analysis: dict):
        """Writes the analysis report to the specified output file."""
        report = {
            "contract_id": self.contract.get("id"),
            "source_contract": str(self.contract_path),
            "analysis_generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_analysis": analysis,
        }

        with open(self.output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        print(f"\n✅ Success! Analysis report saved to: {self.output_path}")

    def run(self):
        """Executes the full schema analysis process."""
        print("\n--- Starting Schema Analysis ---")
        if self.load_contract():
            analysis_results = self.analyse_schema()
            self.write_report(analysis_results)
        print("--- Analysis Complete ---")


def main():
    """Main entry point for the script, with flexible command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Analyse the schema of a Bitol data contract."
    )
    parser.add_argument(
        "--contract", help="Path to the source data contract YAML file.", required=True
    )
    parser.add_argument(
        "--output", help="Path to save the output analysis JSON file.", required=True
    )

    args = parser.parse_args()

    analyser = SchemaAnalyser(contract_path=args.contract, output_path=args.output)
    analyser.run()


if __name__ == "__main__":
    main()
