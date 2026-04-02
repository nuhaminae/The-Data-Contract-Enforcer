# contracts/runner.py
# Validates data against generated contracts.
# RUN Example: python contracts/runner.py --contract generated_contracts/week1_intent_records.yaml --data outputs/week1/intent_records.jsonl --output validation_reports/week1_validation.json

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import yaml


class ValidationRunner:
    """
    A class to encapsulate the logic for validating data against a YAML contract.
    """

    def __init__(self, contract_path, data_path):
        """
        Initializes the runner with paths to the contract and data files.

        Args:
            contract_path (Path): The path to the YAML contract file.
            data_path (Path): The path to the JSONL data file.
        """
        self.contract_path = contract_path
        self.data_path = data_path
        self.contract = None
        self.data = None
        self.report = {}

    def load_contract(self):
        """Loads the YAML contract file into memory."""
        print(f"  - Loading contract: {self.contract_path}")
        if not self.contract_path.exists():
            print(f"ERROR: Contract file not found.")
            return False
        with open(self.contract_path, "r") as f:
            try:
                self.contract = yaml.safe_load(f)
                return True
            except yaml.YAMLError as e:
                print(f"ERROR: Could not parse YAML contract. {e}")
                return False

    def load_data(self):
        """Loads the JSONL data file into memory."""
        print(f"  - Loading data: {self.data_path}")
        records = []
        if not self.data_path.exists():
            print(f"ERROR: Data file not found.")
            return False
        with open(self.data_path, "r") as f:
            for i, line in enumerate(f):
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"WARNING: Skipping malformed JSON on line {i+1}")
        self.data = records
        return True

    def validate(self):
        """
        Performs the core validation logic against the loaded data and contract.
        """
        print("  - Performing validation...")
        if "schema" not in self.contract:
            self.report = {"error": "Contract is missing 'schema' definition."}
            return

        schema = self.contract["schema"]
        required_fields = set(schema.keys())
        errors = []
        total_records = len(self.data)
        valid_records = 0

        for i, record in enumerate(self.data):
            record_errors = []

            # 1. Check for missing fields
            missing_fields = required_fields - set(record.keys())
            if missing_fields:
                record_errors.append(
                    {"error_type": "missing_fields", "fields": list(missing_fields)}
                )

            # 2. Check field types
            for field, expected_type_str in schema.items():
                if field in record:
                    actual_type = type(record[field]).__name__
                    if actual_type != expected_type_str:
                        record_errors.append(
                            {
                                "error_type": "type_mismatch",
                                "field": field,
                                "expected": expected_type_str,
                                "found": actual_type,
                            }
                        )

            if not record_errors:
                valid_records += 1
            else:
                errors.append({"record_index": i, "details": record_errors})

        # Assemble the validation result
        self.report = {
            "validation_summary": {
                "total_records_checked": total_records,
                "valid_records": valid_records,
                "invalid_records": len(errors),
                "pass_rate": (
                    (valid_records / total_records) if total_records > 0 else 0
                ),
            },
            "errors": errors,
        }

    def save_report(self, output_path):
        """Saves the generated validation report to a file."""
        print(f"  - Saving report to: {output_path}")
        # Add final metadata before saving
        final_report = {
            "report_metadata": {
                "contract_file": str(self.contract_path),
                "data_file": str(self.data_path),
                "report_generated_at": datetime.now(timezone.utc).isoformat(),
            },
            **self.report,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(final_report, f, indent=2)

    def run(self):
        """
        Executes the full validation workflow: load, validate, and return the report.
        """
        if not self.load_contract() or not self.load_data():
            print("\nValidation halted due to file loading errors.")
            return None

        self.validate()
        return self.report


def main():
    """Main function to parse arguments and use the ValidationRunner class."""
    parser = argparse.ArgumentParser(description="A Data Contract Validator.")

    parser.add_argument(
        "--contract", type=Path, required=True, help="Path to the YAML contract file."
    )
    parser.add_argument(
        "--data",
        type=Path,
        required=True,
        help="Path to the JSONL data file to validate.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to write the JSON validation report.",
    )

    args = parser.parse_args()

    print(f"--- Starting Data Contract Validation ---")

    # 1. Instantiate the class with the required paths
    runner = ValidationRunner(contract_path=args.contract, data_path=args.data)

    # 2. Execute the validation workflow
    report = runner.run()

    # 3. Save the result if the run was successful
    if report:
        runner.save_report(args.output)
        print(f"\n--- Validation Complete ---")
        print(f"  - Report saved to: {args.output}")
    else:
        print("\n--- Validation Failed ---")


if __name__ == "__main__":
    main()
