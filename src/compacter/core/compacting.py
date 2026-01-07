import math
import pyarrow as pa

from collections import defaultdict

def compact_tables(tables, target_size_bytes):
    table = pa.concat_tables(tables)
    num_rows = table.num_rows

    # avg row size
    total_size = sum(t.nbytes for t in tables)
    bytes_per_row = total_size / num_rows

    rows_per_file = max(1, int(target_size_bytes / bytes_per_row))
    num_files = math.ceil(num_rows / rows_per_file)

    compacted = []
    for i in range(num_files):
        start = i * rows_per_file
        length = min(rows_per_file, num_rows - start)
        compacted.append(table.slice(start, length))

    return compacted

def group_by_transaction_type(keys):
    groups = defaultdict(list)
    for key in keys:
        if "transaction_type=" not in key:
            continue
        ttype = key.split("transaction_type=")[1].split("/")[0]
        groups[ttype].append(key)
    return groups