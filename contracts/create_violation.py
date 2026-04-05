# contracts/create_violation.py
# This script intentionally introduces a schema violation into a data file for testing purposes.

import argparse
import json
from pathlib import Path

class ViolationCreator:
    """
    Reads a JSONL data file, introduces a schema violation, and writes to a new file.
    """

    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def load_and_violate(self) -> list:
        """Loads the records and introduces a specific violation."""
        print(f"  - Loading data from: {self.input_path}")
        try:
            with open(self.input_path, "r", encoding="utf-8") as f:
                records = [json.loads(line) for line in f if line.strip()]
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"    - Error loading data: {e}")
            return []

        if len(records) < 2:
            print("    - Warning: Not enough records in the file to inject a violation. Needs at least 2.")
            return records

        # --- The Violation Logic ---
        # We will target the second record (index 1) for a clear schema violation.
        # The 'week2-verdicts' contract requires the 'overall_verdict' field.
        # We will delete this key from the record.
        
        target_record = records[1]
        
        if "overall_verdict" in target_record:
            print("  - Injecting violation: Deleting 'overall_verdict' from the second record.")
            del target_record["overall_verdict"]
        else:
            print("    - Warning: 'overall_verdict' not found in the target record. Adding a different violation.")
            # As a fallback, violate another required field.
            if "target_ref" in target_record:
                del target_record["target_ref"]

        return records

    def write_output(self, records: list):
        """Writes the modified records to the output JSONL file."""
        with open(self.output_path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        print(f"\n✅ Success! Wrote {len(records)} records with violation to: {self.output_path}")

    def run(self):
        """Executes the full violation creation process."""
        print("\n--- Starting Violation Injection ---")
        violated_records = self.load_and_violate()
        if violated_records:
            self.write_output(violated_records)
        print("--- Injection Complete ---")


def main():
    """Main entry point for the script with a standardized CLI."""
    parser = argparse.ArgumentParser(
        description="Inject a schema violation into a data file for testing the contract runner."
    )
    parser.add_argument(
        "--input", 
        help="Path to the source data file to violate (e.g., outputs/week2/verdict_record.jsonl).", 
        required=True
    )
    parser.add_argument(
        "--output",
        help="Path to save the new, violated data file.",
        required=True
    )

    args = parser.parse_args()

    creator = ViolationCreator(
        input_path=args.input,
        output_path=args.output
    )
    creator.run()


if __name__ == "__main__":
    main()
