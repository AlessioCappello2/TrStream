import time
import random
import signal

from .config.load_config import load_config
from .config.logging_config import setup_logging
from .core.payment_intent_generator import create_payment_intent

####################################################################
# Logging
####################################################################
logger = setup_logging()
logger.info("Stripe generator service starting...")

####################################################################
# Handle SIGTERM/SIGINT Exceptions
####################################################################
running = True

def shutdown_handler(signum, frame):
    global running
    logger.warning("Shutdown signal received!")
    running = False

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# Main function
def main():
    ####################################################################
    # Config reading
    ####################################################################
    rate = load_config()['rate']

    #####################################################################
    # Events generation
    #####################################################################
    count = 0
    while running:
        intent = create_payment_intent()
        count += 1

        logger.debug(
            f"PaymentIntent created | id={intent.id} "
            f"amount={intent.amount} status={intent.status}"
        )

        if running: # avoid to wait in case of enforced shutdown since the start of the current iteration
            time.sleep(random.uniform(rate['min_sleep_sec'], rate['max_sleep_sec']))


if __name__ == "__main__":
    main()
