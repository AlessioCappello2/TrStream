import pyarrow as pa
import pyarrow.compute as pc

def normalize_timestamp(table):
    return table.set_column(
        table.schema.get_field_index("timestamp"),
        "timestamp",
        pc.strptime(
            pa.array([s.split(".")[0] for s in table["timestamp"].to_pylist()]),
            format="%Y-%m-%dT%H:%M:%S",
            unit="s",
        ),
    )


def split_partitions(table):
    timestamps = table.column("timestamp")
    transaction_types = table.column("transaction_type")

    results = []

    for ttype in set(transaction_types.to_pylist()):
        by_type = table.filter(pc.field("transaction_type") == ttype)

        dates = {ts.date() for ts in timestamps.to_pylist()}

        for date in dates:
            part = by_type.filter(pc.day(pc.field("timestamp")) == date.day)
            results.append((date.year, date.month, date.day, ttype, part))

    return results
