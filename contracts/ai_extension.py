# contracts/ai_extension.py
# This script uses a local LLM to enrich a data contract by adding missing descriptions.

import argparse
import time
from pathlib import Path

import requests  # To communicate with the local Ollama server
import yaml

# --- Ollama and Path Setup ---
OLLAMA_ENDPOINT = "http://localhost:11434/api/generate"
MODEL_NAME = "Qwen2.5:3B"  # The specific local model to use.

try:
    current_script_path = Path(__file__).resolve()
    PROJECT_ROOT = current_script_path.parents[5]
except NameError:
    PROJECT_ROOT = Path.cwd()

print(f"Project root directory: {PROJECT_ROOT}")
print(f"Using local Ollama model: {MODEL_NAME}")


# --- Local LLM Function for enriching schema ---
def get_llm_inferred_description(
    column_name: str, column_type: str, contract_id: str
) -> str:
    """
    Asks the local Ollama model to generate a one-sentence description for a data column.
    """
    print(f"   > Inferring description for column: '{column_name}'...")

    prompt = f"""
    Given a data contract column with the following details:
    - Contract ID: '{contract_id}'
    - Column Name: '{column_name}'
    - Data Type: '{column_type}'

    Generate a concise, one-sentence business-level description for this column.
    
    ONE-SENTENCE DESCRIPTION:
    """

    try:
        payload = {"model": MODEL_NAME, "prompt": prompt, "stream": False}
        response = requests.post(OLLAMA_ENDPOINT, json=payload)
        response.raise_for_status()

        description = (
            response.json()
            .get("response", f"A {column_type} field named {column_name}.")
            .strip()
        )
        time.sleep(0.5)  # Be kind to your local machine
        return description
    except requests.exceptions.ConnectionError:
        print("      - LLM Error: Could not connect to Ollama server. Is it running?")
        return (
            f"Purpose inference failed: Ollama server not reachable for {column_name}."
        )
    except Exception as e:
        print(f"      - LLM Error for {column_name}: {e}")
        return f"Purpose inference failed due to an API error for {column_name}."


class DataContractAIAssistant:
    """
    Uses an AI model to enrich a data contract with missing descriptions.
    """

    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.contract = None

    def load_contract(self) -> bool:
        """Loads the source YAML data contract."""
        print(f"  - Loading contract: {self.input_path}")
        try:
            with open(self.input_path, "r", encoding="utf-8") as f:
                self.contract = yaml.safe_load(f)
            return True
        except (FileNotFoundError, yaml.YAMLError) as e:
            print(f"    - Error loading contract: {e}")
            return False

    def enrich_schema(self):
        """
        Iterates through the schema and uses the LLM to fill in missing descriptions.
        """
        print("  - Analysing schema for missing descriptions...")
        schema = self.contract.get("schema", {})
        contract_id = self.contract.get("id", "unknown_contract")

        if not schema:
            print("    - No schema found to enrich.")
            return

        for column_name, details in schema.items():
            # Check if description is missing, empty, or a generic placeholder.
            description = details.get("description", "")
            if not description or "Inferred" in description:
                new_description = get_llm_inferred_description(
                    column_name, details.get("type", "unknown"), contract_id
                )
                self.contract["schema"][column_name]["description"] = new_description
                print(f"      - ✅ Enriched '{column_name}': {new_description}")

    def write_contract(self):
        """Writes the enriched contract to the new output file."""
        with open(self.output_path, "w", encoding="utf-8") as f:
            yaml.dump(
                self.contract,
                f,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )
        print(f"\n✅ Success! Enriched contract saved to: {self.output_path}")

    def run(self):
        """Executes the full enrichment process."""
        print("\n--- Starting AI Contract Enrichment ---")
        if self.load_contract():
            self.enrich_schema()
            self.write_contract()
        print("--- Enrichment Complete ---")


def main():
    """Main entry point for the script with a standardised CLI."""
    parser = argparse.ArgumentParser(
        description="Enrich a Bitol data contract with AI-generated descriptions."
    )
    parser.add_argument(
        "--input",
        help="Path to the source data contract YAML file to enrich.",
        required=True,
    )
    parser.add_argument(
        "--output",
        help="Path to save the new, enriched data contract YAML file.",
        required=True,
    )

    args = parser.parse_args()

    assistant = DataContractAIAssistant(input_path=args.input, output_path=args.output)
    assistant.run()


if __name__ == "__main__":
    main()
