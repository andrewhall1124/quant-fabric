import polars as pl

# @feature
def momentum(chunk: pl.DataFrame):
    feature = (
        chunk
        .with_columns(
            pl.col('ret').log1p().alias("logret")
        )
        .with_columns(
            pl.col("logret")
            .rolling_sum(window_size=11, min_periods=11, center=False)
            .over("ticker")
            .alias("mom")
        )
    )
    return feature