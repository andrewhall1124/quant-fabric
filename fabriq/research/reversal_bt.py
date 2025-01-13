from datetime import date

from fabriq.research.backtester import Backtester
from fabriq.shared.enums import Interval
from fabriq.shared.strategies import ReversalStrategy

bt = Backtester(
    start_date=date(2020, 8, 1),
    end_date=date.today(),
    interval=Interval.DAILY,
    strategy=ReversalStrategy,
)
bt.run()
