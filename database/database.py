import polars as pl
import os


class Database:
    def __init__(self):
        self._tables_dir = "database/.tables/"

    def create(self, table_name: str, data: pl.DataFrame) -> None:
        table_path = os.path.join(self._tables_dir, f"{table_name}.parquet")
        data.write_parquet(table_path)

    def read(self, table_name: str) -> pl.DataFrame:
        table_path = os.path.join(self._tables_dir, f"{table_name}.parquet")
        return pl.read_parquet(table_path)

    def delete(self, table_name: str) -> None:
        table_path = os.path.join(self._tables_dir, f"{table_name}.parquet")
        os.remove(table_path)
