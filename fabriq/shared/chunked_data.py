import polars as pl
from tqdm import tqdm

from fabriq.shared.strategies.strategy import Strategy


class ChunkedData:
    def __init__(self, data: pl.DataFrame, window: int, columns: list[str]):
        self.window = window

        unique_dates = data.select("date").unique().sort(by="date")["date"].to_list()
        chunks = []

        for i in tqdm(range(window, len(unique_dates) + 1), desc="Chunking data"):

            start_date = unique_dates[i - window]
            end_date = unique_dates[i - 1]

            chunk = data.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
            ).select(columns)

            chunks.append(chunk)

        self._chunks: list[pl.DataFrame] = chunks

    def apply_strategy(self, strategy: Strategy) -> list[pl.DataFrame]:
        portfolios_list = []
        for chunk in tqdm(self._chunks, desc="Running strategy"):
            portfolios = strategy.compute_portfolio(chunk)
            if not portfolios.is_empty():
                portfolios_list.append(portfolios)
        return portfolios_list

    @property
    def chunks(self) -> list[pl.DataFrame]:
        return self._chunks
