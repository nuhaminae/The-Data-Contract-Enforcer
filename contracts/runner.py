# contracts/runner.py
# This script validates a data source against a given data contract.

import argparse
import json

import pandas as pd
import yaml


class ValidationReport:
    """A simple class to hold the results of a validation run."""

    def __init__(self, contract_id, data_source, status="pass", message=""):
        self.contract_id = contract_id
        self.data_source = data_source
        self.status = status
        self.message = message
        self.checks = []

    def to_dict(self):
        return {
            "status": self.status,
            "message": self.message,
            "checks": self.checks,
        }


def validate_profile(df: pd.DataFrame, schema: dict) -> ValidationReport:
    """
    Validates a DataFrame against a schema profile, creating a report.
    """
    report = ValidationReport(contract_id="", data_source="")  # Placeholder IDs
    all_checks_passed = True

    # Check for missing columns defined in the contract
    contract_cols = set(schema.keys())
    data_cols = set(df.columns)
    missing_cols = contract_cols - data_cols

    if missing_cols:
        all_checks_passed = False
        report.checks.append(
            {
                "status": "fail",
                "message": f"Missing columns in data source: {', '.join(missing_cols)}",
            }
        )

    for col_name, rules in schema.items():
        if col_name not in df.columns:
            continue

        # Check for nulls if column is required
        if rules.get("required") and df[col_name].isnull().any():
            all_checks_passed = False
            report.checks.append(
                {
                    "status": "fail",
                    "message": f"Column '{col_name}' is required but contains null values.",
                }
            )
        else:
            report.checks.append(
                {
                    "status": "pass",
                    "message": f"Column '{col_name}' nullability check passed.",
                }
            )

    if all_checks_passed:
        report.status = "pass"
        report.message = "All schema checks passed."
    else:
        report.status = "fail"
        report.message = "One or more schema checks failed."

    return report


class ValidationRunner:
    """
    Validates a data source against a data contract.
    """

    def __init__(self, contract_path: str, data_path: str, output_path: str = None):
        self.contract_path = contract_path
        self.data_path = data_path
        self.contract = None
        self.data = None
        self.output_path = output_path
        self.report = None

    def load_contract(self) -> bool:
        """
        Load the data contract from the specified file.
        """
        print(f"  - Loading contract: {self.contract_path}")
        try:
            with open(self.contract_path, "r", encoding="utf-8") as f:
                self.contract = yaml.safe_load(f)
            return True
        except Exception as e:
            print(f"    - Error loading contract: {e}")
            return False

    def load_data(self) -> bool:
        """
        Load the source data from the specified file.
        """
        print(f"  - Loading data: {self.data_path}")
        records = []
        try:
            with open(self.data_path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"    - Warning: Skipping malformed JSON line: {i+1}")
            self.data = pd.json_normalize(records, sep="_")
            return True
        except Exception as e:
            print(f"    - Error loading data: {e}")
            return False

    def validate_schema(self) -> ValidationReport:
        """
        Validate the schema of the data source against the contract.
        """
        print("  - Validating schema...")
        if self.contract is None or self.data is None:
            return ValidationReport(
                contract_id=self.contract.get("id", "unknown"),
                data_source=self.data_path,
                status="error",
                message="Contract or data not loaded.",
            )

        contract_schema = self.contract.get("schema", {})
        # This now calls the local function defined at the top of the script.
        report = validate_profile(self.data, contract_schema)

        # Print results
        for check in report.checks:
            if check.get("status") == "pass":
                print(f"    - ✅ {check.get('message')}")
            else:
                print(f"    - ❌ {check.get('message')}")

        return report

    def validate_quality(self):
        """
        Validate the data quality of the data source against the contract.
        """
        print("  - Validating quality (Soda checks)...")
        print("    - ✅ Soda checks passed (simulated).")

    def generate_report(self, schema_report: ValidationReport):
        """
        Generate a validation report.
        """
        print("  - Generating report...")
        final_status = "pass" if schema_report.status == "pass" else "fail"

        report = {
            "contract_id": self.contract.get("id"),
            "data_source": self.data_path,
            "status": final_status,
            "schema_validation": schema_report.to_dict(),
            "quality_validation": {
                "status": "pass",
                "message": "Soda checks passed (simulated).",
            },
        }

        self.report = report

        if self.output_path:
            with open(self.output_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2)
            print(f"  - Report saved to: {self.output_path}")

    def run(self):
        """
        Run the data contract validation.
        """
        print("\n--- Starting Data Contract Validation ---")
        if not self.load_contract() or not self.load_data():
            print("--- Validation failed: Could not load files. ---")
            return

        schema_report = self.validate_schema()
        self.validate_quality()
        self.generate_report(schema_report)

        print("--- Validation Complete ---")
        return self.report


def main():
    """
    Main entry point for the script.
    """
    parser = argparse.ArgumentParser(
        description="Validate a data source against a data contract."
    )
    parser.add_argument(
        "--contract", help="Path to the data contract file.", required=True
    )
    parser.add_argument("--data", help="Path to the data source file.", required=True)
    parser.add_argument(
        "--output",
        help="Path to save the validation report JSON file.",
        required=False,
    )

    args = parser.parse_args()

    runner = ValidationRunner(
        contract_path=args.contract, data_path=args.data, output_path=args.output
    )
    report = runner.run()

    if report and report.get("status") == "fail":
        pass


if __name__ == "__main__":
    main()
