import os
from datetime import date, timedelta

import polars as pl
from alpaca.data import StockHistoricalDataClient
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.models.bars import BarSet
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading import TradingClient
from dotenv import load_dotenv
import exchange_calendars as ecals
from tqdm import tqdm

from fabriq.shared.database import Database
from fabriq.shared.datasets.alpaca_assets import AlpacaAssets
from fabriq.shared.enums import Interval


class AlpacaStock:

    def __init__(
        self,
        start_date: date,
        end_date: date,
        interval: Interval,
        redownload: bool = False,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.interval = interval
        self.redownload = redownload
        self.download_intervals = []

        if not end_date:
            end_date = date.today()

        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET_KEY")

        self._stock_client = StockHistoricalDataClient(api_key, secret_key)
        self._trading_client = TradingClient(api_key, secret_key)
        self.db = Database()

        # Create the core table if it doesn't already exist
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
        if not self.redownload:
            print("Getting download intervals")
            self._get_download_intervals()

        if not self.download_intervals:
            print("All data already downloaded")
            return

        for download_interval in tqdm(
            self.download_intervals, desc="Downloading missing data"
        ):
            self.cur_start_date = download_interval["start"]
            self.cur_end_date = download_interval["end"]

            start_str = self.cur_start_date.strftime("%Y-%m-%d")
            end_str = self.cur_end_date.strftime("%Y-%m-%d")

            print("-" * 20 + f" {start_str} -> {end_str} " + "-" * 20)
            try:
                self._download_and_stage()
                self._transform()
                self._merge()

            except ValueError as e:
                print(e)

    def load(self) -> pl.DataFrame:

        data = self.db.read(self.core_table_name)

        data = data.sort(by=["ticker", "date"])

        data = data.filter(
            pl.col("date") >= self.start_date, pl.col("date") <= self.end_date
        )

        data = data.with_columns(
            [pl.col("close").pct_change().over("ticker").alias("ret")]
        )

        return data

    def _get_download_intervals(self):
        dates = (
            self.db.read(self.core_table_name).select("date").unique().sort(by="date")
        )

        nyse = ecals.get_calendar("XNYS")
        schedule = nyse.sessions_in_range(self.start_date, self.end_date).to_list()
        schedule = (
            pl.DataFrame(schedule)
            .rename({"column_0": "date"})
            .with_columns(pl.col("date").dt.date())
        )

        # Get missing dates
        if self.interval == Interval.MONTHLY:
            schedule = schedule.with_columns(pl.col("date").dt.truncate("1mo")).unique()

        missing_dates = schedule.join(dates, on="date", how="anti").sort(by="date")

        if self.interval == Interval.DAILY:
            missing_dates = missing_dates.with_columns(
                pl.col("date").dt.truncate("1mo")
            ).unique()

            self.download_intervals = (
                missing_dates.with_columns(
                    pl.col("date").alias("start"),
                    pl.col("date").dt.offset_by("1mo").alias("end"),
                )
                .drop("date")
                .to_dicts()
            )

        elif self.interval == Interval.MONTHLY:
            missing_dates = missing_dates.with_columns(
                pl.col("date").dt.truncate("1y")
            ).unique()

            self.download_intervals = missing_dates.with_columns(
                pl.col("date").alias("start"),
                pl.col("date").dt.offset_by("1y").alias("end"),
            ).to_dicts()

        else:
            raise ValueError(f"Interval '{self.interval.value}' not supported.")

    def _download_and_stage(
        self,
    ):
        if self.db.exists(f"{self.table_name}_STG"):
            return

        # Get tradable symbols
        tickers = self._get_tickers()

        timeframe_unit = (
            TimeFrameUnit.Day
            if self.interval == Interval.DAILY
            else TimeFrameUnit.Month
        )

        # Get stock bars request
        stock_bar_request = StockBarsRequest(
            symbol_or_symbols=tickers,
            timeframe=TimeFrame(1, timeframe_unit),
            start=self.cur_start_date,
            end=self.cur_end_date,
            adjustment=Adjustment.SPLIT,
            feed=DataFeed.IEX,
        )
        print("Downloading Alpaca data")
        bar_set: BarSet = self._stock_client.get_stock_bars(stock_bar_request)

        # Parsing
        data = bar_set.df.reset_index()
        stage_table = pl.from_pandas(data)

        if stage_table.is_empty():
            raise ValueError("Empty dataframe")

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

    @property
    def table_name(self):
        start = self.cur_start_date.strftime("%Y-%m-%d")
        end = self.cur_end_date.strftime("%Y-%m-%d")
        return f"ALPACA_STOCK_{self.interval.value}_{start}_{end}"

    @property
    def core_table_name(self):
        return f"ALPACA_STOCK_{self.interval.value}"


if __name__ == "__main__":
    start = date(2022, 1, 1)
    end = date(2024, 12, 31)
    dataset = AlpacaStock(
        start_date=start, end_date=end, interval=Interval.DAILY
    ).download()
