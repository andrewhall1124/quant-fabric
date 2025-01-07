from qcomponents import ChunkedData
from src.signals import momentum_signal
from src.datasets import AlpacaStockMonthly
from src.optimizers import decile_portfolio
from functools import partial
from datetime import date


def momentum_strategy(type: str = "daily"):
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
    portfolios = chunked_data.apply_portfolio_gen(partial(decile_portfolio, signal='mom'))    

    return portfolios

if __name__ == "__main__":
    portfolios = momentum_strategy("monthly")
    print(portfolios[-1])
