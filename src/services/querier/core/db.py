import time
import duckdb 
from typing import Dict, Any
from pathlib import Path

from querier.config.settings import settings
from shared.config.logging_config import setup_logging
from shared.config.load_config import load_config_from_directory

logger = setup_logging(service_name="trstream.querier")

def init_duckdb(config: Dict[str, Any] = None, retries: int = None, delay: int = None) -> duckdb.DuckDBPyConnection:
    """
    Initialize DuckDB with S3/MinIO configuration.
    
    Args:
        config: Optional config dict (defaults to loading from YAML)
        retries: Optional max retry attempts (defaults to config value)
        delay: Optional retry delay in seconds (defaults to config value)
        
    Returns:
        Configured DuckDB connection
        
    Raises:
        RuntimeError: If initialization fails after all retries
    """
    if config is None:
        config = load_config_from_directory(Path("src"), "querier.yaml")

    # DB init config
    db_init = config.get("db_init", {})
    retries = retries if retries is not None else db_init.get("max_retries", 3)
    delay = delay if delay is not None else db_init.get("retry_delay", 5)

    # DuckDB config
    duckdb_config = config.get("duckdb", {})
    use_ssl = duckdb_config.get('use_ssl', False)
    http_keep_alive = duckdb_config.get('http_keep_alive', False)

    for attempt in range(retries):
        try: 
            con = duckdb.connect(database=':memory:')

            # Clean endpoint
            endpoint = settings.minio_endpoint.replace("https://", "").replace("http://", "")

            statement = f"""
                INSTALL 'httpfs';
                LOAD 'httpfs';

                SET s3_endpoint='{endpoint}';
                SET s3_url_style='path';         
                SET s3_access_key_id='{settings.minio_access_key}';
                SET s3_secret_access_key='{settings.minio_secret_key}';
                SET s3_use_ssl={'false' if not use_ssl else 'true'};
                SET http_keep_alive={'false' if not http_keep_alive else 'true'};
            """

            con.execute(statement)

            logger.info("DuckDB initialized successfully")
            logger.info(f"Connected to MinIO at {endpoint}")
            return con
        except Exception as e:
            logger.warning(f"DuckDB init failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    raise RuntimeError(f"DuckDB initialization failed after {retries} attempts")
