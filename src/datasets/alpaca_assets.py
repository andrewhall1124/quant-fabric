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


class AlpacaAssets:

    def __init__(self, overwrite: bool = False) -> None:
        self.overwrite = overwrite

        load_dotenv()
        api_key = os.getenv("ALPACA_API_KEY")
        secret_key = os.getenv("ALPACA_API_SECRET_KEY")

        self._table_name = "ALPACA_ASSETS"
        self._trading_client = TradingClient(api_key, secret_key)
        self.db = Database()

    def _already_downloaded(self) -> bool:
        return self.db.exists(self._table_name)

    def download(self):
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
                "ticker": asset.symbol,
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
                "attributes": ", ".join(asset.attributes) if asset.attributes else None,
            }
            for asset in assets
        ]
        data = pl.DataFrame(parsed_assets)

        data = data.filter(
            pl.col("status") == "active",
            pl.col("tradable") == True,
            pl.col("fractionable") == True,
            pl.col("shortable") == True,
        )

        self.db.create(self._table_name, data, overwrite=self.overwrite)

    def load(self):

        if not self._already_downloaded():
            self.download()

        return self.db.read(self._table_name)
