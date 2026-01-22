import sys
import logging

class IgnoreHealthFilter(logging.Filter):
    # Avoid log-flooding for healthchecks
    def filter(self, record):
        return "/health" not in record.getMessage()
    

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | [%(levelname)s] | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    logging.getLogger("uvicorn.access").addFilter(IgnoreHealthFilter())
    return logging.getLogger("trstream.stripe.webhook")
