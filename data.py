import yfinance as yf
import polars as pl
import os


class ToyDataset:

    def __init__(self):
        self.data_dir = "data/"
        self.raw_file_path = self.data_dir + "raw_toy_dataset.parquet"
        self.clean_file_path = self.data_dir + "clean_toy_dataset.parquet"

        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

        if not os.path.exists(self.clean_file_path):
            if not os.path.exists(self.raw_file_path):
                self.download()

            self.clean()

    def download(self):
        tickers = ["AAPL", "F", "KO", "VZ"]

        data = yf.download(tickers, "2023-01-01", "2024-12-31")

        data = data.stack(future_stack=True).reset_index()

        data = pl.from_pandas(data)

        data.write_parquet(self.raw_file_path)

    def clean(self):
        df = pl.read_parquet(self.raw_file_path)

        df = df.rename({col: col.lower() for col in df.columns})

        df = df.sort(by=["ticker", "date"])

        df = (
            df.with_columns(
                pl.col('date').dt.strftime("%Y-%m").alias('mdt')
            )
            .group_by(['mdt', 'ticker'])
            .agg(
                date=pl.col('date').last(),
                close=pl.col('close').last()
            )
            .sort(['ticker', 'date'])
        ).select(['date', 'ticker', 'close'])

        df = df.with_columns([pl.col("close").pct_change().over('ticker').alias("ret")]).drop_nulls()

        df.write_parquet(self.clean_file_path)

    def load(self):
        return pl.read_parquet(self.clean_file_path)
    

if __name__ == "__main__":
    data = ToyDataset().load()
    print(data)
