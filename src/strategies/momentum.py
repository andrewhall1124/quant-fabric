from qcomponents import ChunkedData
from src.features import momentum_feature
from src.datasets import ToyDataset
from functools import partial


def momentum_strategy(type: str = "daily"):
    """
    This is the script for the classic momentum trading strategy.
    It should be able to be passed to both a backtester and a live/paper trader.
    """

    # Set window size
    match type:
        case "daily":
            window = 230
        case "monthly":
            window = 11

    # Pull raw data
    raw_data = ToyDataset(type).load()

    # Create chunks
    chunked_data = ChunkedData(raw_data, 11, ["date", "ticker", "ret"])

    # Apply feature transformations
    chunked_data.apply_feature(partial(momentum_feature, type=type))

    # Generate portfolios

    # Return orders

    print(chunked_data.chunks[-1].drop_nulls())


if __name__ == "__main__":
    strategy = momentum_strategy("monthly")
