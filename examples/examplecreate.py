import time
from threading import Thread

import rxbp
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.testing.tobserver import TObserver


def demo1():
    def work(o, skd):
        for i in range(10):
            o.on_next([i])
        o.on_completed()

    source = rxbp.create(work)
    source = source.pipe(
        rxbp.op.observe_on(scheduler=AsyncIOScheduler()),
        # rxbp.op.observe_on(scheduler=EventLoopScheduler()),
        # rxbp.op.observe_on(scheduler=ThreadPoolScheduler("receiver")),
        rxbp.op.subscribe_on(scheduler=ThreadPoolScheduler("publisher")),
    )

    sink = TObserver(immediate_continue=4)
    # source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))
    source.subscribe(observer=sink)

    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4]

    print("-" * 80)
    sink.immediate_continue += 2
    sink.ack.on_next(continue_ack)
    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7]

    print("-" * 80)
    sink.immediate_continue += 1
    sink.ack.on_next(continue_ack)
    time.sleep(1)
    assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def demo2():
    def counter(sink):
        while True:
            time.sleep(5)
            print(f"[**client**] received: ", sink.received)

    def work(o, skd):
        for i in range(100_000):
            o.on_next([i])
        o.on_completed()

    source = rxbp.create(work)
    # no back-pressure until the buffer is full
    source = source.pipe(
        rxbp.op.buffer(10)
    )

    sink = TObserver(immediate_continue=4)
    source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))

    t1 = Thread(target=counter, args=(sink,))
    t1.start()
    t1.join()


if __name__ == '__main__':
    demo1()

    # demo2()
