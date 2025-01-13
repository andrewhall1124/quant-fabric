from datetime import date

from fabriq.research.backtester import Backtester
from fabriq.shared.enums import Interval
from fabriq.shared.strategies import MomentumStrategy

print("\n" + "-" * 50 + " Backtest P&L " + "-" * 50)


bt = Backtester(
    start_date=date(2020, 1, 1),
    end_date=date.today(),
    interval=Interval.MONTHLY,
    strategy=MomentumStrategy,
)
bt.run()
