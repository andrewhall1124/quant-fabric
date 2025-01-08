from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.models.bars import BarSet
from alpaca.trading import TradingClient
from alpaca.trading import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.trading.models import Asset
from datetime import date
import os
from dotenv import load_dotenv
import polars as pl
from qdatabase import Database
from src.datasets.alpaca_assets import AlpacaAssets


class AlpacaStock:

    def __init__(
        self,
        start_date: date,
        end_date: date | None = date.today(),
        interval: str = "daily",
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval

        start = start_date.strftime("%Y-%m-%d")
        end = end_date.strftime("%Y-%m-%d")
        self.table_name = f"ALPACA_STOCK_{interval.upper()}_{start}_{end}"

        if not end_date:
            end_date = date.today()

        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET_KEY")

        self._stock_client = StockHistoricalDataClient(api_key, secret_key)
        self._trading_client = TradingClient(api_key, secret_key)
        self.db = Database()

        # Create the core table if it doesn't already exist
        self.core_table_name = f"ALPACA_STOCK_{self.interval.upper()}"
        core_table_schema = {
            "ticker": pl.Utf8,
            "date": pl.Date,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
            "trade_count": pl.Float64,
            "vwap": pl.Float64,
        }
        empty_core_table = pl.DataFrame(schema=core_table_schema)
        self.db.create(
            table_name=self.core_table_name, data=empty_core_table, overwrite=False
        )

    def download(self):
        self._download_and_stage()
        self._transform()
        self._merge()

    def load(self) -> pl.DataFrame:
        core_table_name = f"ALPACA_STOCK_{self.interval.upper()}"
        data = self.db.read(core_table_name)

        data = data.sort(by=["ticker", "date"])

        data = data.filter(
            pl.col("date") >= self.start_date, pl.col("date") <= self.end_date
        )

        data = data.with_columns(
            [pl.col("close").pct_change().over("ticker").alias("ret")]
        )

        return data

    def _download_and_stage(self):
        if self.db.exists(f"{self.table_name}_STG"):
            return

        # Get tradable symbols
        tickers = self._get_tickers()

        timeframe_unit = (
            TimeFrameUnit.Day if self.interval == "daily" else TimeFrameUnit.Month
        )

        # Get stock bars request
        stock_bar_request = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame(1, timeframe_unit),
            start=self.start_date,
            end=self.end_date,
            adjustment=Adjustment.SPLIT,
            feed=DataFeed.IEX,
        )
        print("Downloading Alpaca data")
        bar_set: BarSet = self._stock_client.get_stock_bars(stock_bar_request)

        # Parsing
        data = bar_set.df.reset_index()
        stage_table = pl.from_pandas(data)

        # Create stage table
        print("Staging")
        self.db.create(f"{self.table_name}_STG", stage_table)

    def _transform(self):
        print("Transforming")
        xf_table = self.db.read(f"{self.table_name}_STG")

        # Rename columns
        xf_table = xf_table.rename({"symbol": "ticker", "timestamp": "date"})

        # Cast date type
        xf_table = xf_table.with_columns(pl.col("date").dt.date())

        xf_table = xf_table.sort(by=["ticker", "date"])

        self.db.create(f"{self.table_name}_XF", xf_table)

    def _merge(self):
        xf_table = self.db.read(f"{self.table_name}_XF")
        core_table = self.db.read(self.core_table_name)

        # Find unique rows
        unique_rows = xf_table.join(core_table, on=["date", "ticker"], how="anti")

        print(unique_rows)

        # Insert unique rows into core table
        print(f"Inserting {len(unique_rows)} unique rows")
        self.db.insert(self.core_table_name, unique_rows)

        # Archive stage table
        self.db.archive(f"{self.table_name}_STG")

        # Delete xf table
        self.db.delete(f"{self.table_name}_XF")

    def _get_tickers(self):
        print("Getting available assets")
        assets = AlpacaAssets().load()
        return assets["ticker"].to_list()


if __name__ == "__main__":
    years = range(2024, 2025)  # Unsure why years 2016-2019 don't have data...
    # for year in years:
    #     for month in range(1,13):
    year = 2024
    month = 12
    print("-" * 10 + f" {year}-{month} " + "-" * 10)
    start = date(year, month, 1)
    end = date(year, month, 31)
    dataset = AlpacaStock(start_date=start, end_date=end, interval="daily").download()

# TODO: Adapt the dataset to check which dates are already downloaded before running (make an option 'redownload')
