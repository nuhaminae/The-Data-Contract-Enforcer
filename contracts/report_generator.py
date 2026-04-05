# contracts/report_generator.py
# This script reads all validation and analysis reports and generates a consolidated summary.

import argparse
import json
from datetime import datetime
from pathlib import Path


class ConsolidatedReportGenerator:
    """
    Generates a consolidated Markdown report from validation and analysis JSON files.
    """

    def __init__(self, validation_dir: str, analysis_dir: str, output_file: str):
        self.validation_dir = Path(validation_dir)
        self.analysis_dir = Path(analysis_dir)
        self.output_file = Path(output_file)
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.combined_data = {}

    def load_reports(self):
        """Loads all JSON reports from the specified directories."""
        print("  - Loading validation reports...")
        for report_path in self.validation_dir.glob("*.json"):
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    contract_id = data.get("contract_id")
                    if contract_id:
                        if contract_id not in self.combined_data:
                            self.combined_data[contract_id] = {}
                        self.combined_data[contract_id]["validation"] = data
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"    - Warning: Could not load or parse {report_path}: {e}")

        print("  - Loading analysis reports...")
        for report_path in self.analysis_dir.glob("*.json"):
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    contract_id = data.get("contract_id")
                    if contract_id:
                        if contract_id not in self.combined_data:
                            self.combined_data[contract_id] = {}
                        self.combined_data[contract_id]["analysis"] = data
            except (json.JSONDecodeError, FileNotFoundError) as e:
                print(f"    - Warning: Could not load or parse {report_path}: {e}")

    def generate_markdown_report(self):
        """Generates the full Markdown report string."""
        print("  - Generating Markdown report...")
        report_lines = []

        # --- Header ---
        report_lines.append("# Data Contract Compliance and Schema Report")
        report_lines.append(
            f"_Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
        )
        report_lines.append("\n---")

        # --- Summary Table ---
        report_lines.append("## Overall Status Summary")
        report_lines.append(
            "| Contract ID | Validation Status | Total Columns | Required Columns |"
        )
        report_lines.append("|---|---|---|---|")

        sorted_contracts = sorted(self.combined_data.keys())

        for contract_id in sorted_contracts:
            data = self.combined_data[contract_id]
            val_report = data.get("validation", {})
            an_report = data.get("analysis", {}).get("schema_analysis", {})

            status_icon = "✅ Pass" if val_report.get("status") == "pass" else "❌ Fail"
            total_cols = an_report.get("total_columns", "N/A")
            req_cols = an_report.get("required_columns_count", "N/A")

            report_lines.append(
                f"| `{contract_id}` | {status_icon} | {total_cols} | {req_cols} |"
            )

        report_lines.append("\n---")

        # --- Detailed Sections ---
        report_lines.append("## Detailed Findings per Contract")

        for contract_id in sorted_contracts:
            report_lines.append(f"\n### Contract: `{contract_id}`")
            data = self.combined_data[contract_id]
            val_report = data.get("validation", {})
            an_report = data.get("analysis", {}).get("schema_analysis", {})

            # Validation Details
            if val_report.get("status") == "fail":
                report_lines.append("#### ❌ Validation Failures")
                schema_checks = val_report.get("schema_validation", {}).get(
                    "checks", []
                )
                failed_checks = [
                    check for check in schema_checks if check.get("status") == "fail"
                ]
                if failed_checks:
                    for check in failed_checks:
                        report_lines.append(f"- {check.get('message')}")
                else:
                    report_lines.append(
                        "- No specific validation failures found in the report."
                    )
            else:
                report_lines.append("#### ✅ Validation Passed")
                report_lines.append("- All schema and quality checks passed.")

            # Schema Analysis Details
            report_lines.append("\n#### Schema Analysis")
            undescribed = an_report.get("undescribed_columns_count", 0)
            if undescribed > 0:
                report_lines.append(
                    f"- **Warning:** Found **{undescribed}** columns missing a description."
                )
                for col in an_report.get("undescribed_columns", []):
                    report_lines.append(f"  - `{col}`")
            else:
                report_lines.append("- All columns have descriptions.")

            type_counts = an_report.get("column_type_counts", {})
            if type_counts:
                types_str = ", ".join(
                    [f"{count} {type}" for type, count in type_counts.items()]
                )
                report_lines.append(f"- Column Types: {types_str}.")

        return "\n".join(report_lines)

    def write_report(self, markdown_content: str):
        """Writes the final report to the output file."""
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write(markdown_content)
        print(f"\n✅ Success! Consolidated report saved to: {self.output_file}")

    def run(self):
        """Executes the full report generation process."""
        print("\n--- Starting Consolidated Report Generation ---")
        self.load_reports()
        if not self.combined_data:
            print("No reports found to process. Exiting.")
            return

        markdown_report = self.generate_markdown_report()
        self.write_report(markdown_report)
        print("--- Report Generation Complete ---")


def main():
    """Main entry point for the script with a standardised CLI."""
    parser = argparse.ArgumentParser(
        description="Generate a consolidated report from contract validation and analysis files."
    )
    parser.add_argument(
        "--validation-dir",
        help="Directory containing the validation report JSON files.",
        required=True,
    )
    parser.add_argument(
        "--analysis-dir",
        help="Directory containing the schema analysis JSON files.",
        required=True,
    )
    parser.add_argument(
        "--output",
        help="Path to save the output consolidated Markdown report.",
        required=True,
    )

    args = parser.parse_args()

    reporter = ConsolidatedReportGenerator(
        validation_dir=args.validation_dir,
        analysis_dir=args.analysis_dir,
        output_file=args.output,
    )
    reporter.run()


if __name__ == "__main__":
    main()
