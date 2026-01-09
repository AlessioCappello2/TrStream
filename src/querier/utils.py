import re

FORBIDDEN_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "create", "replace"
}

VALID_ALIAS_RE = r"^[A-Za-z][A-Za-z0-9_]*$"

def is_safe_query(sql: str) -> bool:
    normalized = sql.strip().lower()
    return (
        (normalized.startswith("select") or normalized.startswith("with"))
        and not any(k in normalized for k in FORBIDDEN_KEYWORDS)
    )

def validate_alias(name: str) -> bool:
    return re.fullmatch(VALID_ALIAS_RE, name) is not None

def clean_error_message(e: Exception) -> str:
    msg = str(e)
    lines = [line.strip() for line in msg.split("\n") if line.strip()]
    return lines[0] if lines else msg
    
    # prefixes = {
    #     r"^DuckDB Error:\s*",
    #     r"^HTTP Error:\s*",
    #     r"^Catalog Error:\s*",
    #     r"^IO Error:\s*",
    #     r"^Binder Error:\s*",
    #     r"^Parser Error:\s*",
    #     r"^\[\d+\]:\s*",
    #     r"^\d{3}\s*[:\-]\s*",
    # }

    # for p in prefixes:
    #     msg = re.sub(p, '', msg)
