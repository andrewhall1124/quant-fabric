from qdatabase import Database
import polars as pl
from polars.testing import assert_frame_equal
import os

data = pl.DataFrame(
    [
        {"a": 1, "b": 2, "c": 3},
        {"a": 2, "b": 3, "c": 4},
        {"a": 3, "b": 4, "c": 5},
    ]
)

db = Database()


def test_create():
    db.create("test", data)
    assert os.path.exists("database/.tables/test.parquet")


def test_read():
    test_data = db.read("test")
    assert_frame_equal(test_data, data)


def test_delete():
    db.delete("test")
    assert not os.path.exists("database/.tables/test.parquet")
