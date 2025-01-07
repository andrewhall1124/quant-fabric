import polars as pl
from typing import Self


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

    @property
    def chunks(self) -> list[pl.DataFrame]:
        return self._chunks
