import duckdb
from querier.core.security import is_safe_query, validate_alias

class QueryManager:
    """Manages query execution and alias registration."""
    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def execute_query(self, sql: str) -> dict:
        """
        Execute a read-only SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            Dict with 'rows' count and 'data' list of records
            
        Raises:
            ValueError: If query is not safe (not SELECT/WITH)
        """
        if not is_safe_query(sql):
            raise ValueError("Only SELECT or WITH queries are allowed")

        df = self.con.execute(sql).fetchdf()
        return {
            "rows": len(df),
            "data": df.to_dict(orient="records")
        }

    def register_alias(self, name: str, path: str):
        """
        Register a table alias (macro) for a Parquet path.
        
        Args:
            name: Alias name
            path: S3/MinIO path to Parquet files (without 's3://' prefix)
            
        Raises:
            ValueError: If alias name format is invalid
        """
        if not validate_alias(name):
            raise ValueError("Alias name must start with a letter and contain only letters, numbers or underscores")

        self.con.execute(
            f"CREATE OR REPLACE MACRO {name}() AS "
            f"TABLE SELECT * FROM read_parquet('s3://{path}');"
        )
