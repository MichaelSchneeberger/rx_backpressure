from typing import Any

from rxbackpressurebatched.observable import Observable
from rxbackpressurebatched.observer import Observer
from rxbackpressurebatched.scheduler import SchedulerBase


class NowObservable(Observable):
    def __init__(self, elem: Any):
        self.elem = elem

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
        def action(_, __):
            def gen_single_elem():
                yield self.elem

            observer.on_next(gen_single_elem)
            observer.on_completed()

        return subscribe_scheduler.schedule(action)

