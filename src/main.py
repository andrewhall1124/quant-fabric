import polars as pl
from src.data import ToyDataset
from components import ChunkedData
from features import momentum

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
    data = ToyDataset(type='monthly').load()
    chunked_data = ChunkedData(data, 11, ["date", "ticker", "ret"])

    print(len(chunked_data.chunks), "chunks")

    chunked_data.apply_feature(momentum)

    print(chunked_data.chunks[-1].drop_nulls())


