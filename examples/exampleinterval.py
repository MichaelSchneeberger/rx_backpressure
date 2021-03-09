import time
from threading import Thread

import rxbp
from rxbp.schedulers.timeoutscheduler import TimeoutScheduler
from rxbp.testing.tobserver import TObserver


def counter(sink):
    while True:
        time.sleep(1)
        print(f"immediate: {sink.immediate_continue}, received: ", sink.received)


publisher = rxbp.interval(0.5).pipe(
    rxbp.op.map(lambda v: "data_" + str(v))
)
sink = TObserver(immediate_continue=12)
publisher.subscribe(observer=sink, subscribe_scheduler=TimeoutScheduler())

t1 = Thread(target=counter, args=(sink,))
t1.start()
t1.join()
