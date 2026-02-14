import re

FORBIDDEN_KEYWORDS = {
    "insert", "update", "delete", "drop", "alter", "create", "replace", "truncate"
}

VALID_ALIAS_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

def is_safe_query(sql: str) -> bool:
    """
    Check if SQL query is safe (read-only).
    
    Only SELECT and WITH queries are allowed. Queries containing
    write operations (INSERT, UPDATE, DELETE, etc.) are rejected.
    
    Args:
        sql: SQL query string
        
    Returns:
        True if query is safe (SELECT or WITH), False otherwise
    """
    normalized = sql.strip().lower()
    return (
        (normalized.startswith("select") or normalized.startswith("with"))
        and not any(k in normalized for k in FORBIDDEN_KEYWORDS)
    )

def validate_alias(name: str) -> bool:
    """
    Validate alias name format.
    
    Alias names must:
    - Start with a letter (a-z, A-Z)
    - Contain only letters, numbers, and underscores
    
    Args:
        name: Alias name to validate
        
    Returns:
        True if valid, False otherwise
    """
    return VALID_ALIAS_RE.fullmatch(name) is not None

def clean_error_message(e: Exception) -> str:
    """
    Extract clean error message from exception.
    
    DuckDB errors can be multi-line. This extracts just the
    first non-empty line for cleaner API responses.
    
    Args:
        e: Exception to process
        
    Returns:
        Cleaned error message (first non-empty line)
    """
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
