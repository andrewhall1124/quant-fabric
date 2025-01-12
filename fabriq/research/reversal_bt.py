from fabriq.research.backtester import Backtester
from fabriq.shared.strategies import ReversalStrategy
from fabriq.shared.enums import Interval
from datetime import date

bt = Backtester(
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    interval=Interval.DAILY,
    strategy=ReversalStrategy,
)
bt.run()
