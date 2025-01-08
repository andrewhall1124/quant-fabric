from qcomponents import ChunkedData
from src.signals import reversal_signal
from src.datasets import AlpacaStock
from src.optimizers import decile_portfolio
from functools import partial
from datetime import date
import polars as pl

import time

class Timer:
    def __init__(self):
        self.temp = time.time()

    def log(self, action):
        now = time.time()
        statement = f"{action} after {now - self.temp:.2f} seconds"
        self.temp = now
        print(statement)

timer = Timer()


def reversal_strategy(interval: str = "daily") -> list[pl.DataFrame]:
    """
    This is the script for the classic short term reversal trading strategy.
    It should be able to be passed to both a backtester and a live/paper trader.
    """

    # Pull raw data
    raw_data = AlpacaStock(
        start_date=date(2020, 1, 1), end_date=date(2024, 12, 31), interval="daily"
    ).load()

    timer.log("Data loaded")

    # Create chunks
    chunked_data = ChunkedData(raw_data, 23, ["date", "ticker", "ret"])
    timer.log("Data chunked")

    # Apply signal transformations
    chunked_data.apply_signal_transform(partial(reversal_signal, interval=interval))
    chunked_data.remove_chunks()
    timer.log("Applied signal transformations")

    # Generate portfolios
    portfolios_list: list[list[pl.DataFrame]] = chunked_data.apply_portfolio_gen(
        partial(decile_portfolio, signal="rev")
    )
    timer.log("Portfolios generated")

    # Long poor reversal, short good reversal
    portfolios_list = [
        pl.concat(
            [portfolios[0], portfolios[9].with_columns(pl.col("weight") * -1)]
        ).drop(["bin", "rev"])
        for portfolios in portfolios_list
    ]
    timer.log("Long short created")

    return portfolios_list
