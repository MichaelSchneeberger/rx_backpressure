from rxbp.ack.ackimpl import Continue, Stop, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import SchedulerBase, ExecutionModel, Scheduler


class RepeatFirstObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler):
        self._source = source
        self._scheduler = scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
        source = self

        class RepeatFirstObserver(Observer):
            def on_next(self, v):
                def first_val_gen():
                    yield next(v())

                def action(_, __):
                    while True:
                        ack = observer.on_next(first_val_gen)

                        if isinstance(ack, Continue):
                            pass
                        elif isinstance(ack, Stop):
                            break
                        else:
                            def _(ack):
                                if isinstance(ack, Continue):
                                    source._scheduler.schedule(action)

                            ack.subscribe(_)
                            break

                source._scheduler.schedule(action)
                return stop_ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return observer.on_completed()

        repeat_first_observer = RepeatFirstObserver()
        repeat_first_subscription = observer_info.copy(repeat_first_observer)
        return self._source.observe(repeat_first_subscription)
