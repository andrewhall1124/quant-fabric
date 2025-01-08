from qcomponents import ChunkedData
from src.signals import momentum_signal
from src.datasets import AlpacaStockMonthly
from src.optimizers import decile_portfolio
from functools import partial
from datetime import date
import polars as pl


def momentum_strategy(type: str = "daily") -> list[pl.DataFrame]:
    """
    This is the script for the classic momentum trading strategy.
    It should be able to be passed to both a backtester and a live/paper trader.
    """

    # Pull raw data
    raw_data = AlpacaStockMonthly(
        start_date=date(2020,1,1),
        end_date=date(2024,12,31)
    ).load()

    # Create chunks
    chunked_data = ChunkedData(raw_data, 12, ["date", "ticker", "ret"])

    # Apply signal transformations
    chunked_data.apply_signal_transform(partial(momentum_signal, type=type))
    chunked_data.remove_chunks()

    # Generate portfolios
    portfolios_list: list[list[pl.DataFrame]] = chunked_data.apply_portfolio_gen(partial(decile_portfolio, signal='mom')) 

    # Long good momentum, short poor momentum
    portfolios_list = [
        pl.concat([
            portfolios[0].with_columns(pl.col('weight') * -1),
            portfolios[9]
        ]).drop(['bin', 'mom'])
        for portfolios in portfolios_list
    ]

    return portfolios_list

if __name__ == "__main__":
    result = momentum_strategy("monthly")
    print(result)
