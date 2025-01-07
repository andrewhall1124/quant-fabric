import numpy as np
import polars as pl
from qcomponents import ChunkedData

def decile_portfolio(chunk: ChunkedData, signal: str):

    # Calculate decile percentiles
    percentiles = np.linspace(0.1, 1, 10)
    values = [chunk[signal].quantile(percentile) for percentile in percentiles]

    # Dynamically build the conditions for decile bins
    bins_expr = pl.when(pl.col(signal) <= values[0]).then(0)
    for i in range(1, len(values)):
        bins_expr = bins_expr.when(pl.col(signal) <= values[i]).then(i)
    
    # Add the 'bins' column to the DataFrame
    binned_chunk = chunk.with_columns(bins_expr.alias("bin"))

    # Seperate portfolios
    portfolios = [
        binned_chunk.filter(
            pl.col('bin') == i
        ).select(['date', 'ticker', signal, 'bin'])
        for i in range(10)
    ]

    return portfolios