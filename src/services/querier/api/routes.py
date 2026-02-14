import duckdb
from fastapi import APIRouter, HTTPException

from querier.api.models import QueryRequest, QueryResponse, AliasRequest
from querier.core.query_manager import QueryManager
from querier.core.security import clean_error_message
from shared.config.logging_config import setup_logging

logger = setup_logging(service_name="trstream.querier")
router = APIRouter()

manager: QueryManager = None


def set_manager(query_manager: QueryManager) -> None:
    """
    Set the query manager instance.
    
    Args:
        query_manager: Configured QueryManager instance
    """
    global manager
    manager = query_manager


@router.post("/query", response_model=QueryResponse)
def execute_query(req: QueryRequest):
    """
    Execute a read-only SQL query against DuckDB.
    
    Args:
        req: Query request with SQL statement
        
    Returns:
        Query results with row count and data
        
    Raises:
        HTTPException: 403 for unsafe queries, 400 for DuckDB errors, 500 for unexpected errors
    """
    try:
        logger.debug(f"Executing query: {req.sql[:100]}...")
        result = manager.execute_query(req.sql)
        logger.info(f"Query returned {result['rows']} rows")
        return result
    except ValueError as e:
        logger.warning(f"Unsafe query rejected: {e}")
        raise HTTPException(status_code=403, detail=str(e))
    except duckdb.Error as e:
        logger.error(f"DuckDB error: {e}")
        raise HTTPException(status_code=400, detail=clean_error_message(e))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=clean_error_message(e))


@router.post("/alias")
def define_alias(req: AliasRequest):
    """
    Register a table alias (macro) for a Parquet path.
    
    Args:
        req: Alias request with name and path
        
    Returns:
        Success message
        
    Raises:
        HTTPException: 400 for invalid alias, 500 for unexpected errors
    """
    try:
        logger.info(f"Registering alias '{req.name}' -> '{req.path}'")
        manager.register_alias(req.name, req.path)
        return {"detail": f"Alias '{req.name}' registered successfully"}
    except ValueError as e:
        logger.warning(f"Invalid alias: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error registering alias: {e}")
        raise HTTPException(status_code=500, detail=clean_error_message(e))


@router.get("/health")
def health_check():
    """
    Health check endpoint.
    
    Returns:
        Service status
    """
    return {"status": "healthy", "service": "querier"}