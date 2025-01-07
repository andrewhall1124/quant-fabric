from qcomponents import ChunkedData
from src.features import momentum_feature
from src.datasets import ToyDataset
from src.datasets import AlpacaStockMonthly
from functools import partial
from datetime import date
import polars as pl


def momentum_strategy(type: str = "daily"):
    """
    This is the script for the classic momentum trading strategy.
    It should be able to be passed to both a backtester and a live/paper trader.
    """

    # Set window size
    match type:
        case "daily":
            window = 230
        case "monthly":
            window = 11

    # Pull raw data
    # raw_data = ToyDataset(type).load()
    raw_data = AlpacaStockMonthly(
        start_date=date(2020,1,1),
        end_date=date(2024,12,31)
    ).load()

    print(raw_data.filter(pl.col('ticker') == 'AAPL'))

    # Create chunks
    # chunked_data = ChunkedData(raw_data, 11, ["date", "ticker", "ret"])
    chunked_data = ChunkedData(raw_data, 11, ["date", "ticker", "ret"])

    # Apply feature transformations
    chunked_data.apply_feature(partial(momentum_feature, type=type))

    # Generate portfolios

    # Return orders
    print(len(chunked_data.chunks), "chunks")
    print(chunked_data.chunks)


if __name__ == "__main__":
    strategy = momentum_strategy("monthly")
