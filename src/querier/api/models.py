from pydantic import BaseModel, Field

class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        example="SELECT * FROM read_parquet('s3://tb-transactions/**/*.parquet')"
    )

class QueryResponse(BaseModel):
    rows: int
    data: list[dict]

class AliasRequest(BaseModel):
    name: str = Field(..., example="transactions")
    path: str = Field(..., example="s3://tb-transactions/**/*.parquet")
