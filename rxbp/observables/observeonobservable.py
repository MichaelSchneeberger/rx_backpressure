from rx.concurrency.schedulerbase import SchedulerBase

from rxbp.ack import Continue, Stop, Ack
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observablebase import ObservableBase
from rxbp.observer import Observer


class ObserveOnObservable(ObservableBase):
    def __init__(self, source: ObservableBase, scheduler: SchedulerBase):
        self.source = source
        self.scheduler = scheduler

    def unsafe_subscribe(self, observer: Observer, scheduler: SchedulerBase, subscribe_scheduler: SchedulerBase):
        def on_next(v):
            def action(_, __):
                inner_ack = observer.on_next(v)

                if isinstance(inner_ack, Continue):
                    ack.on_next(inner_ack)
                    ack.on_completed()
                elif isinstance(inner_ack, Stop):
                    ack.on_next(inner_ack)
                    ack.on_completed()
                else:
                    inner_ack.unsafe_subscribe(ack)

            self.scheduler.schedule(action)

            ack = Ack()
            return ack

        def on_error(exc):
            def action(_, __):
                observer.on_error(exc)

            self.scheduler.schedule(action)

        def on_completed():
            def action(_, __):
                observer.on_completed()

            self.scheduler.schedule(action)

        observe_on_observer = AnonymousObserver(on_next=on_next, on_error=on_error,
                                     on_completed=on_completed)
        return self.source.unsafe_subscribe(observe_on_observer, scheduler, subscribe_scheduler)
