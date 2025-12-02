import re

from pydantic import BaseModel

# DATA VALIDATION MODELS
class QueryRequest(BaseModel):
    sql: str

class AliasRequest(BaseModel):
    name: str
    path: str

####################################################################
# UTILITIES
####################################################################

def is_safe_query(sql: str) -> bool:
    normalized = sql.strip().lower()
    forbidden = ["insert", "update", "delete", "drop", "alter", "create", "replace"]
    return (
        (normalized.startswith("select") or 
            normalized.startswith("with")) 
        and not any(f in normalized for f in forbidden)
    )

def clean_error_message(e: Exception) -> str:
    msg = str(e)

    lines = [line.strip() for line in msg.split("\n") if line.strip()]
    msg = msg if not lines else lines[0]

    prefixes = {
        r"^DuckDB Error:\s*",
        r"^HTTP Error:\s*",
        r"^Catalog Error:\s*",
        r"^IO Error:\s*",
        r"^Binder Error:\s*",
        r"^Parser Error:\s*",
        r"^\[\d+\]:\s*",
        r"^\d{3}\s*[:\-]\s*",
    }

    for p in prefixes:
        msg = re.sub(p, '', msg)

    return msg