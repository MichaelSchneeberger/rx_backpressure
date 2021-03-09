import time
from threading import Thread

import rxbp
from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.overflowstrategy import DropOld, ClearBuffer, Drop
from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.schedulers.timeoutscheduler import TimeoutScheduler
from rxbp.testing.tobserver import TObserver


def demo1():
    def counter(sink):
        while True:
            time.sleep(5)
            print(f"[**client**] received: ", sink.received)

    publisher = rxbp.interval(0.5).pipe(
        # rxbp.op.strategy(DropOld(buffer_size=15))
        rxbp.op.strategy(ClearBuffer(buffer_size=15))
    )
    sink = TObserver(immediate_continue=5)
    publisher.subscribe(observer=sink, subscribe_scheduler=TimeoutScheduler())

    t1 = Thread(target=counter, args=(sink,))
    t1.start()
    t1.join()


def demo2():
    def counter(sink):
        while True:
            time.sleep(5)
            print(f"[**client**] received: ", sink.received)

    def work(o, skd):
        for i in range(1_000):
            o.on_next([i])
        o.on_completed()

    source = rxbp.create(work)
    source = source.pipe(
        rxbp.op.strategy(DropOld(buffer_size=100)),
        # rxbp.op.strategy(ClearBuffer(buffer_size=15)),
        # rxbp.op.strategy(Drop(buffer_size=15)),
    )

    sink = TObserver(immediate_continue=5)
    source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))

    t1 = Thread(target=counter, args=(sink,))
    t1.start()
    t1.join()


if __name__ == '__main__':
    # demo1()

    demo2()
