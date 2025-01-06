from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.enums import Adjustment, DataFeed
from alpaca.data.models.bars import BarSet
from alpaca.trading import TradingClient
from alpaca.trading import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus, AssetExchange
from alpaca.trading.models import Asset
from datetime import datetime
import os
from dotenv import load_dotenv
import polars as pl
from qdatabase import Database

class AlpacaStock():

    def __init__(self, start_date: datetime, end_date: datetime | None = None) -> None:
        self.start_date = start_date
        self.end_date = end_date

        if not end_date:
            end_date = datetime.today()

        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET_KEY")

        self._stock_client = StockHistoricalDataClient(api_key, secret_key)
        self._trading_client = TradingClient(api_key, secret_key)
        self.db = Database()

        # self._table_name
        self._download_and_stage()
        self._merge()

    def _download_and_stage(self):
        # Get tradable symbols
        symbols = self._get_symbols()

        # Get stock bars request
        stock_bar_request = StockBarsRequest(
            symbol_or_symbols=symbols,
            timeframe=TimeFrame(1, TimeFrameUnit.Day),
            start=self.start_date,
            end=self.end_date,
            adjustment=Adjustment.SPLIT,
            feed=DataFeed.IEX,
        )
        bar_set: BarSet = self._stock_client.get_stock_bars(stock_bar_request)

        # Parsing
        df = bar_set.df.reset_index()
        df = pl.from_pandas(df)

    def _merge(self):
        pass

    def _get_symbols(self):
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
    start = datetime(2025,1,1)
    dataset = AlpacaStock(start_date=start)

# TODO: Set up dataset to download, stage, and merge the data for a specified time frame
# TODO: Adapt the dataset to check which dates are already downloaded before running (make an option 'redownload')
# TODO: Portfolio generator
# TODO: Backtester module
