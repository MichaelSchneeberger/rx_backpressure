import unittest

import rxbp
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.testing.tobserver import TObserver
from rxbp.testing.tscheduler import TScheduler


class TestFromRange(unittest.TestCase):
    def setUp(self):
        self.scheduler = TScheduler()

    def test_happy_case(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_range(1, 4).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))
        disposable = subscription.observable.observe(ObserverInfo(
            observer=sink
        ))

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
        self.assertTrue(sink.is_completed)

    def test_batch_size(self):
        sink = TObserver(immediate_continue=0)
        subscription = rxbp.from_range(1, 4, batch_size=2).unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))
        disposable = subscription.observable.observe(ObserverInfo(
            observer=sink
        ))

        self.scheduler.advance_by(1)

        self.assertEqual([1, 2], sink.received)
        self.assertFalse(sink.is_completed)

        sink.ack.on_next(continue_ack)
        self.scheduler.advance_by(1)

        self.assertEqual([1, 2, 3], sink.received)
        self.assertTrue(sink.is_completed)