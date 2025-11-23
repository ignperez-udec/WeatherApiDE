import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VARIABLES_FILE_PATH = os.path.join(BASE_DIR, 'variables.json')

def load_variables() -> dict:
    """Load configuration variables from a JSON file."""
    try:
        with open(VARIABLES_FILE_PATH, 'r') as file:
            variables = json.load(file)
        return variables
    except FileNotFoundError:
        print(f"Configuration file not found at {VARIABLES_FILE_PATH}. Using default values.")
        return {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON from the configuration file at {VARIABLES_FILE_PATH}. Using default values.")
        return {}