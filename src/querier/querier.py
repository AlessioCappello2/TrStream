####################################################################
# IMPORTS #
####################################################################
import os
import re
import duckdb

from fastapi import FastAPI, HTTPException
from src.querier.__utils import is_safe_query, clean_error_message, QueryRequest, AliasRequest

####################################################################
# Env variables
####################################################################
minio_endpoint = os.environ['MINIO_ENDPOINT']
access_key = os.environ['MINIO_ACCESS_KEY']
secret_key = os.environ['MINIO_SECRET_KEY']

# app
app = FastAPI(title='DuckDB Querier Service')

# valid aliases
valid_re = r"^[A-Za-z][A-Za-z0-9_]*$"

# DUCKDB S3 CONFIG
con = duckdb.connect(database=':memory:')
statement = f"""
    INSTALL 'httpfs';
    LOAD 'httpfs';

    SET s3_endpoint='{minio_endpoint.split("http://")[1]}';
    SET s3_url_style='path';         
    SET s3_access_key_id='{access_key}';
    SET s3_secret_access_key='{secret_key}';
    SET s3_use_ssl='false';
    SET http_keep_alive='false';
"""
con.execute(statement)

# using POWERSHELL 7 (using version < 6 won't work because curl was an alias for the Invoke-WebRequest cmdlet)
# curl -X POST "http://localhost:8000/query" -H "Content-Type: application/json" -d "{""sql"":""SELECT * FROM read_parquet('s3://tb-transactions/**/*.parquet');""}"

####################################################################
# APP ENDPOINTS 
####################################################################
@app.post("/query")
def execute_query(req: QueryRequest):
    # Query validation
    if not is_safe_query(req.sql):
        raise HTTPException(status_code=403, detail="Only SELECT or WITH queries allowed!")
    
    try:
        print(req.sql)
        df = con.execute(req.sql).fetchdf()
        return {"rows": len(df), "data": df.to_dict(orient="records")}
    except duckdb.ParserException as e:
        raise HTTPException(status_code=400, detail=f"{e}")
    except duckdb.CatalogException as e:
        raise HTTPException(status_code=404, detail=f"{e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=clean_error_message(e))
    
@app.post("/alias")
def define_alias(req: AliasRequest):
    try:
        # Enforcing alias names must start with a letter and contain only alphanumeric chars and underscores
        if not re.fullmatch(valid_re, req.name):
            raise HTTPException(status_code=400, detail="Alias name must start with a letter and contain only letters, numbers or underscores")

        # A macro acts as an alias (e.g. SELECT * FROM name())
        print(req.path)
        con.execute(f"CREATE OR REPLACE MACRO {req.name}() AS TABLE SELECT * FROM read_parquet('{req.path}');")
        return {"detail": f"Alias '{req.name}' registered successfully for path '{req.path}'"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=clean_error_message(e))