from rx.core import typing

from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.subscriptionobservable import SubscriptionObservable
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class CreateFlowable(FlowableMixin):
    def __init__(
            self,
            observer: typing.Subscription,
    ):
        self._observer = observer

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return init_subscription(
            observable=SubscriptionObservable(
                source=self._observer,
                scheduler=subscriber.scheduler,
                subscribe_scheduler=subscriber.subscribe_scheduler,
            )
        )
