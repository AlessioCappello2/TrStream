import duckdb
from fastapi import FastAPI, HTTPException

from .api.models import QueryRequest, QueryResponse, AliasRequest
from .core.db import init_duckdb
from .core.query_manager import QueryManager
from .utils import clean_error_message

####################################################################
# App, db connection and request manager
####################################################################
app = FastAPI(title="DuckDB Querier Service")
con = init_duckdb()
manager = QueryManager(con)

####################################################################
# APP ENDPOINTS 
####################################################################
@app.post("/query", response_model=QueryResponse)
def execute_query(req: QueryRequest):
    try:
        return manager.execute_query(req.sql)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=clean_error_message(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=clean_error_message(e))

@app.post("/alias")
def define_alias(req: AliasRequest):
    try:
        manager.register_alias(req.name, req.path)
        return {"detail": f"Alias '{req.name}' registered successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=clean_error_message(e))
