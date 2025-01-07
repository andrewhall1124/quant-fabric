import polars as pl

def momentum_signal(chunk: pl.DataFrame, type: str = "daily"):
    # Set window size
    match type:
        case "daily":
            window = 230
        case "monthly":
            window = 11

    # Signal transformation
    signal = chunk.with_columns(pl.col("ret").log1p().alias("logret")).with_columns(
        pl.col("logret")
        .rolling_sum(window_size=window, min_periods=window, center=False)
        .over("ticker")
        .alias("mom")
    )
    return signal
