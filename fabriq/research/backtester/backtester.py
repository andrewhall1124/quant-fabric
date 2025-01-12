from fabriq.shared.datasets import AlpacaStock
from fabriq.shared.chunked_data import ChunkedData
from fabriq.shared.strategies.strategy import Strategy
from datetime import date
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt


class Backtester:

    def __init__(
        self, start_date: date, end_date: date, interval: str, strategy: Strategy
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.strategy = strategy(interval)

    def run(self):
        data = (
            AlpacaStock(
                start_date=self.start_date,
                end_date=self.end_date,
                interval=self.interval,
            )
            .load()
            .select("ticker", "date", "ret")
        )

        # Create chunks
        chunked_data = ChunkedData(
            data=data, window=self.strategy.window, columns=["date", "ticker", "ret"]
        )

        portfolios = chunked_data.apply_strategy(self.strategy)

        portfolios = pl.concat(portfolios)

        merged = data.join(portfolios, how="inner", on=["date", "ticker"])

        merged = merged.with_columns(
            (pl.col("weight") * pl.col("ret")).alias("weighted_ret")
        )

        pnl = (
            merged.group_by("date")
            .agg(weighted_ret_mean=pl.col("weighted_ret").sum())
            .sort(by=["date"])
        )

        pnl = (
            pnl.with_columns(pl.col("weighted_ret_mean").alias("portfolio_ret"))
            .with_columns(pl.col("portfolio_ret").log1p().alias("portfolio_logret"))
            .with_columns(
                ((pl.col("portfolio_ret") + 1).cum_prod() - 1).alias("cumprod"),
                pl.col("portfolio_logret").cum_sum().alias("cumsum"),
            )
        )

        print(pnl)

        # Cumulative product plot
        sns.lineplot(data=pnl, x="date", y="cumprod")
        plt.ylabel("Cummulative returns (product)")
        plt.xlabel("Date")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
