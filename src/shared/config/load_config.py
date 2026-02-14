import yaml

from pathlib import Path
from typing import Dict, Any 


def load_yaml(config_path: Path) -> Dict[str, Any]:
    """
    Load a YAML configuration file.
    
    Args:
        config_path: Path to the YAML file
        
    Returns:
        Parsed configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        yaml.YAMLError: If YAML is invalid
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    if config is None:
        raise ValueError(f"Empty or invalid YAML file: {config_path}")
        
    return config


def load_config_from_directory(config_dir: Path, filename: str = "config.yaml") -> Dict[str, Any]:
    """
    Load config.yaml from a specific directory.
    
    Args:
        config_dir: Directory containing the config file
        filename: Name of the config file (default: config.yaml)
        
    Returns:
        Parsed configuration dictionary
    """
    return load_yaml(config_dir / filename)