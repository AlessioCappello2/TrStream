import time
import duckdb 

from .settings import settings
from .logging_config import setup_logging

logger = setup_logging()

def init_duckdb(retries: int = 5, delay: int = 3) -> duckdb.DuckDBPyConnection:
    for attempt in range(retries):
        try: 
            con = duckdb.connect(database=':memory:')

            statement = f"""
                INSTALL 'httpfs';
                LOAD 'httpfs';

                SET s3_endpoint='{settings.minio_endpoint.replace("https://", "").replace("http://", "")}';
                SET s3_url_style='path';         
                SET s3_access_key_id='{settings.minio_access_key}';
                SET s3_secret_access_key='{settings.minio_secret_key}';
                SET s3_use_ssl={'true' if settings.duckdb_use_ssl else 'false'};
                SET http_keep_alive={'true' if settings.duckdb_http_keep_alive else 'false'};
            """

            con.execute(statement)

            logger.info("DuckDB initialized successfully")
            return con
        except Exception as e:
            logger.warning(f"DuckDB init failed: attempt {attempt+1}/{retries} - {e}")
            if attempt < retries - 1:
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

    raise RuntimeError("DuckDB initialization failed after multiple attempts")