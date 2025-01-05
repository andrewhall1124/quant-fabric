from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.models.bars import BarSet
from datetime import datetime
import os
from dotenv import load_dotenv
import polars as pl

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_API_SECRET_KEY")

stock_client = StockHistoricalDataClient(api_key, secret_key)

stock_bar_request = StockBarsRequest(
    symbol_or_symbols=["AAPL"],
    timeframe=TimeFrame.Day,
    start=datetime(2025, 1, 1),
    end=datetime(2025, 1, 4),
    adjustment=Adjustment.SPLIT,
    feed=DataFeed.IEX,
)

bar_set: BarSet = stock_client.get_stock_bars(stock_bar_request)

df = bar_set.df.reset_index()

df = pl.from_pandas(df)

print(df)
