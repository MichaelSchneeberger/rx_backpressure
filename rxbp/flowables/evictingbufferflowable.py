from dataclasses import dataclass

from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observables.evictingobservable import EvictingObservable
from rxbp.overflowstrategy import OverflowStrategy
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class EvictingBufferFlowable(FlowableMixin):
    source: FlowableMixin
    overflow_strategy: OverflowStrategy
    # buffer_size: int

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        subscription = self.source.unsafe_subscribe(subscriber=subscriber)

        observable = EvictingObservable(
            subscription.observable,
            # buffer_size=self.buffer_size,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
            overflow_strategy=self.overflow_strategy,
        )

        return subscription.copy(
            observable=observable,
        )
