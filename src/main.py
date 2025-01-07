import polars as pl
from src.datasets.toy_dataset import ToyDataset
from qcomponents import ChunkedData
from signals import momentum_signal
from qdatabase import Database

if __name__ == "__main__":
    db = Database()
