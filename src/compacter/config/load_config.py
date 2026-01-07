import yaml

from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime, date, timedelta

def load_config() -> Dict[str, Any]:
    path = Path(__file__).parent / "config.yaml"

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        return yaml.safe_load(f)

def _normalize_date(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(hour=0, minute=0, second=0, microsecond=0)

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d")

    raise TypeError(f"Invalid date value {value} of type {type(value)}")

def resolve_dates(cfg: Dict[str, Any]) -> List[datetime]:
    if "compaction" not in cfg:
        raise ValueError("Missing 'compaction' section in config file")
    
    comp = cfg["compaction"]
    mode = comp.get("mode")

    if mode == "single_day":
        if "date" not in comp:
            raise ValueError("compaction.date is required when mode='single_day'.")
        return [_normalize_date(comp["date"])]
    
    if mode == "range":
        if "start_date" not in comp or "end_date" not in comp:
            raise ValueError("compaction.start_date and compaction.end_date are required when mode='range'.")
        
        start = _normalize_date(comp["start_date"])
        end = _normalize_date(comp["end_date"])

        if start > end:
            raise ValueError("compaction.start_date must be before compaction.end_date.")
        
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]
    
    raise ValueError(f"Invalid compaction mode: {mode}. Expected 'single_day' or 'range'.")