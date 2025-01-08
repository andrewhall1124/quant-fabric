from strategies import momentum_strategy, reversal_strategy
from backtester import Backtester
from functools import partial
from datetime import date

# print("\n" + "-" * 50 + " Last Period Portfolio " + "-" * 50)

# portfolios = reversal_strategy(interval="daily")
# print(portfolios[-1])


print("\n" + "-" * 50 + " Backtest P&L " + "-" * 50)

bt = Backtester(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    interval="daily",
    strategy=partial(reversal_strategy, interval="daily"),
)
bt.run()
