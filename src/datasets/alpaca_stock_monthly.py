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

class AlpacaStockMonthly():

    def __init__(self, start_date: date, end_date: date | None = date.today()) -> None:
        self.start_date = start_date
        self.end_date = end_date

        start = start_date.strftime('%Y-%m-%d')
        end = end_date.strftime('%Y-%m-%d')
        self.table_name = f"ALPACA_STOCK_MONTHLY_{start}_{end}"

        if not end_date:
            end_date = date.today()

        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET_KEY")

        self._stock_client = StockHistoricalDataClient(api_key, secret_key)
        self._trading_client = TradingClient(api_key, secret_key)
        self.db = Database()

    def download(self):
        self._download_and_stage()
        self._transform()
        self._merge()

    def load(self) -> pl.DataFrame:
        core_table_name = 'ALPACA_STOCK_MONTHLY'
        data = self.db.read(core_table_name)

        data = data.sort(by=['ticker', 'date'])

        data = data.filter(
            pl.col('date') >= self.start_date,
            pl.col('date') <= self.end_date
        )

        data = data.with_columns(
            [pl.col("close").pct_change().over("ticker").alias("ret")]
        )

        return data

    def _download_and_stage(self):
        # Get tradable symbols
        symbols = self._get_symbols()

        # Get stock bars request
        stock_bar_request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(1, TimeFrameUnit.Month),
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
        xf_table = xf_table.rename({'symbol': 'ticker', 'timestamp': 'date'})

        # Cast date type
        xf_table = xf_table.with_columns(
            pl.col('date').dt.date()
        )

        xf_table = xf_table.sort(by=["ticker", "date"])

        self.db.create(f"{self.table_name}_XF", xf_table)

    def _merge(self):
        
        # Create the core table if it doesn't already exist
        core_table_name = 'ALPACA_STOCK_MONTHLY'
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
            table_name=core_table_name, 
            data=empty_core_table, 
            overwrite=True
        )

        # Prepare tables for merge with source tracking
        xf_table = self.db.read(f"{self.table_name}_XF").with_columns(pl.lit('xf').alias('source'))
        core_table = self.db.read(core_table_name).with_columns(pl.lit('core').alias('source'))
        
        # Columns to compare on
        unique_columns = core_table_schema.keys()

        # Find unique rows
        unique_rows = (
            pl.concat([core_table, xf_table])
            .unique(subset=unique_columns, keep='first')
            .filter(pl.col('source') == 'xf')
            .drop('source')
        )

        # Insert unique rows into core table
        print(f"Inserting {len(unique_rows)} unique rows")
        self.db.insert(core_table_name, unique_rows)

        # Archive stage table
        self.db.archive(f"{self.table_name}_STG")

        # Delete xf table
        self.db.delete(f"{self.table_name}_XF")

    def _get_symbols(self):
        print("Getting available assets")
        # Alpaca get assets request
        search_params = GetAssetsRequest(
            status=AssetStatus.ACTIVE,
            asset_class=AssetClass.US_EQUITY,
        )
        assets: list[Asset] = self._trading_client.get_all_assets(search_params)

        # Parse raw assets data
        parsed_assets = [
                {
                    "id": str(asset.id),
                    "asset_class": asset.asset_class.value,
                    "exchange": asset.exchange.value,
                    "symbol": asset.symbol,
                    "name": asset.name,
                    "status": asset.status.value,
                    "tradable": asset.tradable,
                    "marginable": asset.marginable,
                    "shortable": asset.shortable,
                    "easy_to_borrow": asset.easy_to_borrow,
                    "fractionable": asset.fractionable,
                    "min_order_size": asset.min_order_size,
                    "min_trade_increment": asset.min_trade_increment,
                    "price_increment": asset.price_increment,
                    "maintenance_margin_requirement": asset.maintenance_margin_requirement,
                    "attributes": ", ".join(asset.attributes) if asset.attributes else None
                }
                for asset in assets
            ]

        # Filter out untradable stocks
        df = pl.DataFrame(parsed_assets)
        df = df.filter(
            pl.col('status') == 'active',
            pl.col('tradable') == True,
            pl.col('fractionable') == True,
            pl.col('shortable') == True
        )

        return df['symbol']

if __name__ == '__main__':
    years = range(2020,2025) # Unsure why years 2016-2019 don't have data...
    for year in years:
        print("-"*10 + f"{year}" + "-"*10)
        start = date(year,1,1)
        end = date(year, 12, 31)
        dataset = AlpacaStockMonthly(
            start_date=start,
            end_date=end
        ).download()

# TODO: Adapt the dataset to check which dates are already downloaded before running (make an option 'redownload')
# TODO: Portfolio generator
# TODO: Backtester module
