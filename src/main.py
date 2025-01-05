import polars as pl
from src.data import ToyDataset
from components import ChunkedData
from features import momentum_feature

if __name__ == "__main__":
    data = ToyDataset(type='monthly').load()
    chunked_data = ChunkedData(data, 11, ["date", "ticker", "ret"])

    print(len(chunked_data.chunks), "chunks")

    chunked_data.apply_feature(momentum_feature)

    print(chunked_data.chunks[-1].drop_nulls())


