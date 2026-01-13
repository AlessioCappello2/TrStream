import sys
import logging

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | [%(levelname)s] | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    logging.getLogger("kafka").setLevel(logging.CRITICAL)
    return logging.getLogger("trstream.stripe.webhook")
