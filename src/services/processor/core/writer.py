import uuid
from datetime import datetime
from shared.minio.writer import BaseS3ParquetWriter


class ProcessorS3ParquetWriter(BaseS3ParquetWriter):
    def _build_key(self, source, year, month, day):
        commit_time = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        uid = uuid.uuid4().hex[:8]

        return (
            f"source={source}/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"part-{commit_time}-{uid}.parquet"
        )

    def write_partition(self, source, year, month, day, table):
        return self.write_table(
            table,
            source=source,
            year=year,
            month=month,
            day=day
        )
