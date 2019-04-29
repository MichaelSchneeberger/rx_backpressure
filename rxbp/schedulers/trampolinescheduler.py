# from rx.concurrency import current_thread_scheduler as parent_current_thread_scheduler
import logging
import threading
import time
from typing import Optional

from rx.concurrency import ScheduledItem
from rx.concurrency.schedulerbase import SchedulerBase
from rx.core import typing
from rx.internal import PriorityQueue
from rx.internal.constants import DELTA_ZERO

from rxbp.scheduler import SchedulerBase as RxBPSchedulerBase

log = logging.getLogger('Rx')


class TrampolineScheduler(RxBPSchedulerBase, SchedulerBase):
    class Trampoline(object):
        @classmethod
        def run(cls, queue: PriorityQueue[ScheduledItem[typing.TState]]) -> None:
            while queue:
                item: ScheduledItem = queue.peek()
                if item.is_cancelled():
                    queue.dequeue()
                else:
                    diff = item.duetime - item.scheduler.now
                    if diff <= DELTA_ZERO:
                        item.invoke()
                        queue.dequeue()
                    else:
                        time.sleep(diff.total_seconds())

    def __init__(self):
        """Gets a scheduler that schedules work as soon as possible on the
        current thread."""

        super().__init__()

        self.idle: bool = True
        self.queue: PriorityQueue[ScheduledItem[typing.TState]] = PriorityQueue()

        self.lock = threading.RLock()

    def schedule(self,
                 action: typing.ScheduledAction,
                 state: Optional[typing.TState] = None
                 ) -> typing.Disposable:
        """Schedules an action to be executed.
        Args:
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        return self.schedule_absolute(self.now, action, state=state)

    def schedule_relative(self,
                          duetime: typing.RelativeTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed after duetime.
        Args:
            duetime: Relative time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        Returns:
            The disposable object used to cancel the scheduled action
            (best effort).
        """

        duetime = SchedulerBase.normalize(self.to_timedelta(duetime))
        return self.schedule_absolute(self.now + duetime, action, state=state)

    def schedule_absolute(self, duetime: typing.AbsoluteTime,
                          action: typing.ScheduledAction,
                          state: Optional[typing.TState] = None
                          ) -> typing.Disposable:
        """Schedules an action to be executed at duetime.
        Args:
            duetime: Absolute time after which to execute the action.
            action: Action to be executed.
            state: [Optional] state to be given to the action function.
        """

        duetime = self.to_datetime(duetime)

        if duetime > self.now:
            log.warning("Do not schedule blocking work!")

        si: ScheduledItem[typing.TState] = ScheduledItem(self, state, action, duetime)

        with self.lock:
            self.queue.enqueue(si)

            if self.idle:
                self.idle = False
                start_trampoline = True
            else:
                start_trampoline = False

        if start_trampoline:
            while True:
                try:
                    TrampolineScheduler.Trampoline.run(self.queue)
                finally:
                    with self.lock:
                        if not self.queue:
                            self.idle = True
                            self.queue.clear()
                            break

        return si.disposable

    def schedule_required(self):
        """Test if scheduling is required.

        Gets a value indicating whether the caller must call a
        schedule method. If the trampoline is active, then it returns
        False; otherwise, if  the trampoline is not active, then it
        returns True.
        """
        return self.queue is None

    def ensure_trampoline(self, action):
        """Method for testing the CurrentThreadScheduler."""

        if self.schedule_required():
            return self.schedule(action)
        else:
            return action(self, None)



# class CurrentThreadSchedulerAdapter(SchedulerBase):
#     @property
#     def now(self):
#         return parent_current_thread_scheduler.now
#
#     def schedule(self, action, state=None):
#         return parent_current_thread_scheduler.schedule(action, state)
#
#     def schedule_relative(self, duetime, action, state=None):
#         return parent_current_thread_scheduler.schedule_relative(duetime, action, state)
#
#     def schedule_absolute(self, duetime, action, state=None):
#         return parent_current_thread_scheduler.schedule_absolute(duetime, action, state)


# current_thread_scheduler = CurrentThreadSchedulerAdapter()