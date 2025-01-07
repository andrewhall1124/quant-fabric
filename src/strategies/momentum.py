from qcomponents import ChunkedData
from src.features import momentum_feature
from src.datasets import ToyDataset
from src.datasets import AlpacaStockMonthly
from functools import partial
from datetime import date
import polars as pl
import numpy as np


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
    chunked_data = ChunkedData(raw_data, 11, ["date", "ticker", "ret"])

    # Apply feature transformations
    chunked_data.apply_feature(partial(momentum_feature, type=type))

    print(chunked_data.chunks[-1].drop_nulls())

    # Generate portfolios
    def decile_momentum_bins(chunk: pl.DataFrame) -> pl.DataFrame:

        # Calculate decile percentiles
        percentiles = np.linspace(0.1, 1, 10)
        values = [chunk['mom'].quantile(percentile) for percentile in percentiles]

        # Dynamically build the conditions for decile bins
        bins_expr = pl.when(pl.col("mom") <= values[0]).then(0)
        for i in range(1, len(values)):
            bins_expr = bins_expr.when(pl.col("mom") <= values[i]).then(i)
        
        # Add the 'bins' column to the DataFrame
        binned_chunk = chunk.with_columns(bins_expr.alias("bin"))

        # Seperate portfolios
        portfolios = [
            binned_chunk.filter(
                pl.col('bin') == i
            ).select(['date', 'ticker', 'mom', 'bin'])
            for i in range(10)
        ]

        return portfolios
    
    portfolios = decile_momentum_bins(chunked_data.chunks[-1].drop_nulls())

    return portfolios

if __name__ == "__main__":
    portfolios = momentum_strategy("monthly")
    print(portfolios)
