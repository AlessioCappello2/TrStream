import uuid
import random

from faker import Faker
from datetime import datetime

class Generator():
    def __init__(self, no_users: int = 10, transaction_types: list[str] = None, statuses: list[str] = None):
        self.fake = Faker()
        self.transaction_types = ["DEBIT", "CREDIT", "REFUND"] if not transaction_types else transaction_types
        self.statuses = ["PENDING", "COMPLETED", "FAILED"] if not statuses else statuses
        self.user_ids = [f"user_{i:04d}" for i in range(no_users)]

    def generate_transaction(self):
        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": random.choice(self.user_ids),
            "card_number": self.fake.credit_card_number(),
            "amount": round(random.uniform(5.00, 500.00), 2),
            "currency": "EUR",
            "timestamp": datetime.now().isoformat(timespec='milliseconds'),
            "transaction_type": random.choice(self.transaction_types),
            "status": random.choice(self.statuses)
        }