from shared.minio.writer import BaseS3ParquetWriter


class CompacterS3ParquetWriter(BaseS3ParquetWriter):
    def _build_key(self, source, year, month, day, index):
        return (
            f"tb-{source}/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"compact-{index:06d}.parquet"
        )

    def write_partition(self, source, year, month, day, index, table):
        return self.write_table(
            table,
            source=source,
            year=year,
            month=month,
            day=day,
            index=index
        )