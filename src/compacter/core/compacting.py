import math
import pyarrow as pa

from typing import List

def compact_tables(tables, target_size_bytes) -> List[pa.Table]:
    """
    Return a list of tables according to compaction criteria
    
    :param tables: tables to compact
    :param target_size_bytes: target size in bytes for each file
    :return: list of tables
    :rtype: List[pa.Table]
    """

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
