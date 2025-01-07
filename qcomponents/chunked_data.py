import polars as pl
from typing import Self


class ChunkedData:
    def __init__(self, data: pl.DataFrame, window: int, columns: list[str]):
        self.window = window

        unique_dates = data.select("date").unique().sort(by="date")["date"]
        chunks = []

        for i, end_date in enumerate(unique_dates[window:], start=window):
            start_date = unique_dates[i - window]

            chunk = data.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date),
            ).select(columns)

            chunks.append(chunk)

        self._chunks = chunks

    def apply_signal_transform(self, signal) -> Self:
        for i, chunk in enumerate(self.chunks):
            self.chunks[i] = signal(chunk)

        return self

    def apply_portfolio_gen(self, portfolio_generator) -> list[pl.DataFrame]:
        portfolios = []
        for chunk in self.chunks:
            portfolios.append(
                portfolio_generator(chunk)
            )
        return portfolios
    
    def clean_chunks(self):
        self._chunks = [
            chunk
            for chunk in self._chunks
            if (chunk['date'].max().year - chunk['date'].min().year) * 12 
               + (chunk['date'].max().month - chunk['date'].min().month) == self.window
        ]


    @property
    def chunks(self) -> list[pl.DataFrame]:
        return self._chunks
    
