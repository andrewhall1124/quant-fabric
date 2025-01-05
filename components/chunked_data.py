import polars as pl
from src.data import ToyDataset


class ChunkedData:
    def __init__(self, data: pl.DataFrame, window: int, columns: list[str]):
        unique_dates = data.select("date").unique().sort(by="date")["date"]
        chunks = []

        for i, end_date in list(enumerate(unique_dates))[window - 1 :]:
            start_date = unique_dates[i - window + 1]

            chunk = data.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date),
            ).select(columns)

            chunks.append(chunk)

        self.chunks = chunks

    def apply_feature(self, feature):
        for i, chunk in enumerate(self.chunks):
            self.chunks[i] = feature(chunk)

        return self.chunks
