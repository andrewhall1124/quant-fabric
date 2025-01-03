import polars as pl
from data import ToyDataset

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

        self.chunks = chunks

    def apply_feature(self, feature):
        for i, chunk in enumerate(self.chunks):
            self.chunks[i] = feature(chunk)

        return self.chunks

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

# @strategy
def monthly_momentum_strategy():
    """ 
    This is the script for the classic momentum trading strategy.
    It should be able to be passed to both a backtester and a live/paper trader.
    """
    # (1) pull data
    # (2) create new chunks
    # (3) apply feature transformations
    # (4) generate latest portfolio
    # (5) send portfolio to trader
    pass

if __name__ == "__main__":
    data = ToyDataset().load()
    chunked_data = ChunkedData(data, 11, ["date", "ticker", "ret"])

    print(len(chunked_data.chunks), "chunks")

    chunked_data.apply_feature(momentum)

    print(chunked_data.chunks[-1].drop_nulls())


