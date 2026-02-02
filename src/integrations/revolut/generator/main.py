import uuid 
import time
import signal
import random
import requests

from .auth.auth_service import get_valid_access_token
from .config.load_config import load_config
from .config.settings import settings
from .config.logging_config import setup_logging

####################################################################
# Logging
####################################################################
logger = setup_logging()
logger.info("Revolut generator service starting...")

####################################################################
# Handle SIGTERM/SIGINT Exceptions
####################################################################
running = True 

def handle_termination(signum, frame):
    global running
    print("Shutdown signal received!", flush=True)
    running = False

signal.signal(signal.SIGTERM, handle_termination)
signal.signal(signal.SIGINT, handle_termination)

####################################################################
# API, cfg and session
####################################################################
REVOLUT_TRANSFER_API = "https://sandbox-b2b.revolut.com/api/1.0/transfer"

cfg = load_config()
CERT_PATH, KEY_PATH = cfg['keys']['cert_path'], cfg['keys']['key_path']
min_sleep_sec, max_sleep_sec = cfg['rate']['min_sleep_sec'], cfg['rate']['max_sleep_sec']
min_amount, max_amount = cfg['amount']['min'], cfg['amount']['max']
currency = cfg['currency']
retries = cfg['retries']

session = requests.Session()
session.cert = (CERT_PATH, KEY_PATH)
session.verify = False

# Main function
def main():
    global running
    try:
        while running:
            # retrieve access token to include in request's header
            access_token = get_valid_access_token()

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            request_id = str(uuid.uuid4())

            payload = {
                "request_id": request_id,
                "source_account_id": settings.revolut_source_account,
                "target_account_id": settings.revolut_target_account,
                "amount": round(random.uniform(min_amount, max_amount), 2),
                "currency": currency
            }

            # invoke Revolut transfer
            success = False
            for attempt in range(retries):
                try:
                    response = session.post(
                        REVOLUT_TRANSFER_API,
                        json=payload,
                        headers=headers,
                        timeout=10
                    )

                    if response.status_code == 429:
                        logger.warning("API Rate limit reached, sleeping 30s...")
                        time.sleep(30)
                        continue

                    if response.ok:
                        success = True
                        break

                except requests.RequestException as e:
                    # exponential backoff with upper bound
                    logger.warning(f"Network error: {e}")
                    time.sleep(min(2**attempt+random.random(), 30))

            if not success:
                raise RuntimeError("Transfer failed after all retries!")

            if not response.ok:
                logger.warning("Transfer failed", extra={
                    "request_id": request_id,
                    "status": response.status_code,
                    "body": response.text
                })
            else:
                logger.info("Transfer created", extra={
                        "request_id": request_id
                    }
                )
            
            if running:  # avoid to wait in case of enforced shutdown since the start of the current iteration
                time.sleep(random.uniform(min_sleep_sec, max_sleep_sec))

    except Exception as e:
        logger.error(f"Error: {e}")

    logger.info("Shutting down...")
    

if __name__ == "__main__":
    main()