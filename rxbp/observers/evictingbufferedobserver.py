import threading
from typing import List, Any, Optional

from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.operators.observeon import _observe_on
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck
from rxbp.observer import Observer
from rxbp.overflowstrategy import OverflowStrategy, DropOld, ClearBuffer, Drop
from rxbp.scheduler import Scheduler


class AtomicAny:
    def __init__(self, lock, init_val):
        self._lock = lock
        self._val = init_val

    def get(self):
        return self._val

    def compare_and_set(self, current, update) -> bool:
        meas = self.get_and_set(update)
        return current == meas

    def get_and_set(self, update) -> Any:
        with self._lock:
            meas = self._val
            self._val = update
        return meas


class AtomicInt(AtomicAny):
    def get_and_increment(self, num: int):
        with self._lock:
            meas = self._val
            self._val = meas + num
        return meas

    def decrement_and_get(self, num: int):
        with self._lock:
            self._val = self._val - num
            meas = self._val
        return meas


class EvictingBufferedObserver(Observer):
    def __init__(self, observer: Observer, scheduler: Scheduler, subscribe_scheduler,
                 strategy: OverflowStrategy):
        self.observer = observer
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.em = scheduler.get_execution_model()
        # self.buffer_size = strategy

        self.last_iteration_ack: Optional[Ack] = None

        self.upstream_is_complete = False
        self.downstream_is_complete = False

        # self.items_to_push = 0
        self.error_thrown = None

        self.lock = threading.RLock()

        self.items_to_push = AtomicInt(lock=self.lock, init_val = 0)
        self.queue = self.Buffer(lock=self.lock, strategy=strategy)

    class Buffer:
        def __init__(self, lock, strategy: OverflowStrategy):
            self.lock = lock
            self.strategy = strategy

            self.buffer_ref = AtomicAny(lock=lock, init_val=(0, []))

        def drain(self) -> List:
            current = self.buffer_ref.get_and_set((0, []))
            return current[1]

        def offer(self, a) -> int:
            current = self.buffer_ref.get()
            length, queue = current

            if length < self.strategy.buffer_size:
                update = (length + 1, queue + [a])
                if not self.buffer_ref.compare_and_set(current, update):
                    return self.offer(a)
                else:
                    return 0
            else:
                if isinstance(self.strategy, DropOld):
                    update = (length, queue[1:] + [a])
                    if not self.buffer_ref.compare_and_set(current, update):
                        return self.offer(a)
                    else:
                        return 1
                elif isinstance(self.strategy, ClearBuffer):
                    update = (1, [a])
                    if not self.buffer_ref.compare_and_set(current, update):
                        return self.offer(a)
                    else:
                        return length
                elif isinstance(self.strategy, Drop):
                    return length
                else:
                    raise Exception('illegal case')

    def on_next(self, elem):
        if self.upstream_is_complete or self.downstream_is_complete:
            return StopAck()
        else:
            dropped = self.queue.offer(elem)
            increment = 1 - dropped
            self.push_to_consumer(increment)
            return continue_ack

    def _on_completed_or_error(self, ex=None):
        if not self.upstream_is_complete and not self.downstream_is_complete:
            self.error_thrown = ex
            self.upstream_is_complete = True
            self.push_to_consumer(1)

    def on_error(self, ex):
        self._on_completed_or_error(ex)

    def on_completed(self):
        self._on_completed_or_error(None)

    def push_to_consumer(self, increment: int):
        if increment != 0:
            current_nr = self.items_to_push.get_and_increment(increment)
        else:
            current_nr = self.items_to_push.get()

        if current_nr == 0:
            def action(_, __):
                self.consumer_run_loop()

            self.scheduler.schedule(action)

    def consumer_run_loop(self):
        def signal_next(next):
            try:
                ack = self.observer.on_next(next)
                return ack
            except:
                raise NotImplementedError

        def signal_complete():
            try:
                self.observer.on_completed()
            except:
                raise NotImplementedError

        def signal_error(ex):
            try:
                self.observer.on_error(ex)
            except:
                raise NotImplementedError

        def go_async(current_queue: List, next_val, next_size: int, ack: Ack, processed: int):
            class AckSingle(Single):
                def on_error(self, exc: Exception):
                    raise NotImplementedError

                def on_next(_, v):
                    if isinstance(v, ContinueAck):
                        next_ack = signal_next(next_val)
                        is_sync = isinstance(ack, ContinueAck) or isinstance(ack, StopAck)
                        next_frame = self.em.next_frame_index(0) if is_sync else 0
                        fast_loop(current_queue, next_ack, processed+next_size, next_frame)
                    elif isinstance(v, StopAck):
                        self.downstream_is_complete = True

            _observe_on(ack, self.scheduler).subscribe(AckSingle())

        def fast_loop(prev_queue: List, prev_ack: Ack, last_processed:int, start_index: int):
            ack = continue_ack if prev_ack is None else prev_ack
            is_first_iteration = isinstance(ack, ContinueAck)
            processed = last_processed
            next_index = start_index
            current_queue = prev_queue

            while not self.downstream_is_complete:
                try:
                    if len(current_queue) == 0:
                        current_queue = self.queue.drain()

                    if len(current_queue) == 0:
                        has_next = False
                    else:
                        next_val = current_queue[0]
                        current_queue = current_queue[1:]
                        has_next = True

                    # fetch size
                    next_size = 1

                    if has_next:
                        if next_index > 0 or is_first_iteration:
                            is_first_iteration = False

                            if isinstance(ack, ContinueAck):
                                ack = signal_next(next_val)
                                if isinstance(ack, StopAck):
                                    self.downstream_is_complete = True
                                    return
                                else:
                                    is_sync = isinstance(ack, ContinueAck)
                                    next_index = self.em.next_frame_index(next_index) if is_sync else 0
                                    processed += next_size
                            elif isinstance(ack, StopAck):
                                self.downstream_is_complete = True
                                return
                            else:
                                go_async(current_queue, next_val, next_size, ack, processed)
                                return
                        else:
                            go_async(current_queue, next_val, next_size, ack, processed)
                            return
                    elif self.upstream_is_complete:
                        current_queue = self.queue.drain()

                        if len(current_queue) == 0:
                            self.downstream_is_complete = True
                            if self.error_thrown is None:
                                signal_complete()
                            else:
                                signal_error(self.error_thrown)
                            return
                    else:
                        self.last_iteration_ack = ack
                        remaining = self.items_to_push.decrement_and_get(processed)

                        processed = 0

                        if remaining == 0:
                            return
                except:
                    raise NotImplementedError

        try:
            fast_loop(prev_queue=[], prev_ack=self.last_iteration_ack, last_processed=0, start_index=0)
        except:
            raise NotImplementedError


