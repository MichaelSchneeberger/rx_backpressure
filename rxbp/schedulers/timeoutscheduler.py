import time

from rx.scheduler import TimeoutScheduler as ParentTimeoutScheduler
from rxbp.scheduler import SchedulerBase


class TimeoutScheduler(SchedulerBase, ParentTimeoutScheduler):
    @property
    def idle(self) -> bool:
        return True

    @property
    def is_order_guaranteed(self) -> bool:
        return True

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)
