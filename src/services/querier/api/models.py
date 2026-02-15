from pydantic import BaseModel, Field

class QueryRequest(BaseModel):
    """Request model for executing SQL queries."""
    sql: str = Field(
        ...,
        example="SELECT * FROM read_parquet('s3://tb-transactions/**/*.parquet')",
        description="SQL query to execute (SELECT or WITH only)"
    )

class QueryResponse(BaseModel):
    """Response model for query results."""
    rows: int = Field(..., description="Number of rows returned")
    data: list[dict] = Field(..., description="Query result data")

class AliasRequest(BaseModel):
    """Request model for registering table aliases."""
    name: str = Field(
        ...,
        example="transactions",
        description="Alias name (must start with letter, alphanumeric + underscore)"
    )
    path: str = Field(
        ...,
        example="s3://tb-transactions/**/*.parquet",
        description="S3/MinIO path to Parquet files"
    )
