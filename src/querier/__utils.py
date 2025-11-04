
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