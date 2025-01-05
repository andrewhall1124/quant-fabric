import polars as pl
from src.datasets.toy_dataset import ToyDataset
from components import ChunkedData
from features import momentum_feature
from database import Database

if __name__ == "__main__":
    db = Database()
