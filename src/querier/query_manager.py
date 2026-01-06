import duckdb
from .utils import is_safe_query, validate_alias

class QueryManager:

    def __init__(self, connection: duckdb.DuckDBPyConnection):
        self.con = connection

    def execute_query(self, sql: str) -> dict:
        if not is_safe_query(sql):
            raise ValueError("Only SELECT or WITH queries are allowed")

        df = self.con.execute(sql).fetchdf()
        return {
            "rows": len(df),
            "data": df.to_dict(orient="records")
        }

    def register_alias(self, name: str, path: str):
        if not validate_alias(name):
            raise ValueError("Alias name must start with a letter and contain only letters, numbers or underscores")

        self.con.execute(
            f"CREATE OR REPLACE MACRO {name}() AS "
            f"TABLE SELECT * FROM read_parquet('{path}');"
        )
