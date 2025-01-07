import polars as pl
import os
import shutil

class Database:
    def __init__(self):
        self._tables_dir = "qdatabase/.tables/"
        self._archive_dir = "qdatabase/.archive/"

        os.makedirs(self._tables_dir, exist_ok=True)
        os.makedirs(self._archive_dir, exist_ok=True)

    def create(self, table_name: str, data: pl.DataFrame, overwrite: bool = False) -> None:
        """
        Method for creating a table in the database. If no data is given, an empty table is created.
        All tables are stored locally as parquet files using polars.
        """
        table_path = self.get_table_path(table_name)

        # Overwrite check
        if self.exists(table_name) and not overwrite:
            return

        # Write non-empty or schema-defined DataFrame   
        if data is not None or data.schema:  
            data.write_parquet(table_path)

        # Write empty dataframe
        else:
            pl.DataFrame().write_parquet(table_path)

    def read(self, table_name: str) -> pl.DataFrame:
        table_path = self.get_table_path(table_name)
        return pl.read_parquet(table_path)
    
    def insert(self, table_name: str, rows: pl.DataFrame) -> None:
        table = self.read(table_name)
        table = pl.concat([table, rows])
        self.create(table_name, table, overwrite=True)

    def archive(self, table_name: str) -> None:
        src_table_path = self.get_table_path(table_name)
        dst_table_path = os.path.join(self._archive_dir, f"{table_name}.parquet")
        shutil.move(src_table_path, dst_table_path)

    def delete(self, table_name: str) -> None:
        table_path = self.get_table_path(table_name)
        os.remove(table_path)

    def get_table_path(self, table_name: str) -> str:
        return os.path.join(self._tables_dir, f"{table_name}.parquet")
    
    def exists(self, table_name: str) -> bool:
        table_path = self.get_table_path(table_name)
        return os.path.exists(table_path)