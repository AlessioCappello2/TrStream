import logging
import sys
from typing import Optional

def setup_logging(
    service_name: str,
    level: int = logging.INFO,
    format: str = "%(asctime)s | [%(levelname)s] | %(message)s",
    datefmt: str = "%Y-%m-%d %H:%M:%S",
    suppress_loggers: Optional[dict[str, int]] = None,
) -> logging.Logger:
    """
    Setup logging configuration for TrStream services.
    
    Args:
        service_name: Name of the service (e.g., "trstream.producer")
        level: Logging level (default: INFO)
        format: Log message format
        datefmt: Date format for logs
        suppress_loggers: Dict of logger names to suppress with their levels
                         e.g., {"kafka": logging.CRITICAL, "urllib3": logging.WARNING}
    
    Returns:
        Configured logger instance for the service
    """
    logging.basicConfig(
        level=level,
        format=format,
        datefmt=datefmt,
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    # Suppress noisy loggers
    if suppress_loggers:
        for logger_name, logger_level in suppress_loggers.items():
            logging.getLogger(logger_name).setLevel(logger_level)
    
    return logging.getLogger(service_name)