import uuid
import random

from faker import Faker
from typing import Dict, Any
from datetime import datetime

class TransactionGenerator():
    def __init__(self, cfg: Dict[str, Any]):
        self.fake = Faker()
        self.transaction_types = cfg['transactions']['types']
        self.statuses = cfg['transactions']['statuses']
        self.min_amount, self.max_amount = cfg['transactions']['amount']['min'], cfg['transactions']['amount']['max']
        self.currency = cfg['transactions']['currency']
        self.user_ids = [f"user_{i:07d}" for i in range(cfg['users']['count'])]

    def generate_transaction(self):
        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": random.choice(self.user_ids),
            "card_number": self.fake.credit_card_number(),
            "amount": round(random.uniform(self.min_amount, self.max_amount), 2),
            "currency": self.currency,
            "timestamp": datetime.now().isoformat(timespec='milliseconds'),
            "transaction_type": random.choice(self.transaction_types),
            "status": random.choice(self.statuses)
        }