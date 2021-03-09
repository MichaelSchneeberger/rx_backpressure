from dataclasses import dataclass

from rx.core import typing
from rx.disposable import BooleanDisposable, CompositeDisposable

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pausablebufferedobserver import PausableBufferedObserver
from rxbp.scheduler import Scheduler


@dataclass
class SubscriptionObservable(Observable):
    source: typing.Subscription
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def observe(self, observer_info: ObserverInfo):
        observer = PausableBufferedObserver(
            underlying=observer_info.observer,
            scheduler=self.scheduler,
            subscribe_scheduler=self.subscribe_scheduler,
        )

        d1 = BooleanDisposable()

        def action(_, __):
            return self.source(observer, self.subscribe_scheduler)

        d2 = self.subscribe_scheduler.schedule(action)
        return CompositeDisposable(d1, d2)
