import yaml

from pathlib import Path
from typing import Dict, Any

def load_config() -> Dict[str, Any]:
    path = Path(__file__).parent / "config.yaml"

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        return yaml.safe_load(f)