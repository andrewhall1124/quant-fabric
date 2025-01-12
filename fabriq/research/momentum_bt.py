from fabriq.shared.strategies import MomentumStrategy
from fabriq.research.backtester import Backtester
from datetime import date
from fabriq.shared.enums import Interval

print("\n" + "-" * 50 + " Backtest P&L " + "-" * 50)


bt = Backtester(
    start_date=date(2020, 1, 1),
    end_date=date(2024, 12, 31),
    interval=Interval.MONTHLY,
    strategy=MomentumStrategy,
)
bt.run()
