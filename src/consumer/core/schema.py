import pyarrow as pa

TRANSACTION_SCHEMA = pa.schema([
    ('transaction_id', pa.string()),
    ('user_id', pa.string()),
    ('card_number', pa.string()),
    ('amount', pa.float32()),
    ('currency', pa.string()),
    ('timestamp', pa.string()),
    ('transaction_type', pa.string()),
    ('status', pa.string())
])
