from fastapi import FastAPI

from querier.api import routes
from querier.core.db import init_duckdb
from querier.core.query_manager import QueryManager

from shared.config.logging_config import setup_logging

logger = setup_logging(service_name="trstream.querier")

####################################################################
# App, db connection and request manager
####################################################################
logger.info("Initializing FastAPI app...")
app = FastAPI(
    title="TrStream Querier Service", 
    description="SQL query interface for DuckDB with S3/MinIO backend"
)

logger.info("Initializing DuckDB connection...")
con = init_duckdb()

logger.info("Initializing query manager...")
manager = QueryManager(con)

####################################################################
# Set manager and routes
####################################################################
routes.set_manager(manager)
app.include_router(routes.router)

logger.info("Querier service ready!")