from strategies import momentum_strategy
from backtester import Backtester
from functools import partial
from datetime import date

print("\n" + "-"*50 + "Last Period Portfolio" + "-" * 50)

portfolios = momentum_strategy(type="monthly")

print(portfolios[-1])

print("\n" + "-"*50 + "Backtest P&L" + "-" * 50)

bt = Backtester(
    start_date=date(2020,1,1),
    end_date=date(2024,12,31),
    strategy=partial(momentum_strategy, type="monthly")
)

bt.run()

