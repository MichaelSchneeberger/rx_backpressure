import threading
from dataclasses import dataclass
from typing import Optional

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.operators.observeon import _observe_on
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.states.measuredstates.bufferedstates import BufferedStates
from rxbp.states.rawstates.rawbufferedstates import RawBufferedStates
from rxbp.typing import ElementType


@dataclass
class PausableBufferedObserver(Observer):
    underlying: Observer
    scheduler: Scheduler
    subscribe_scheduler: Scheduler

    def __post_init__(self):
        self.em = self.scheduler.get_execution_model()

        self.lock = threading.RLock()

        self.state: RawBufferedStates.State = RawBufferedStates.InitialState(meas_state=None, last_ack=continue_ack)
        self.queue = []

        self.last_ack = None

    def _start_loop(self, last_ack: Optional[Ack], next_index: int):
        def schedule_ack(ack: Ack, next: ElementType):
            outer_self = self

            class ResultSingle(Single):
                def on_next(self, ack: Ack):
                    if isinstance(ack, ContinueAck):
                        last_ack = outer_self.underlying.on_next(next)
                        outer_self.last_ack = last_ack

                        with outer_self.lock:
                            outer_self.queue.pop(0)
                            len_queue = len(outer_self.queue)
                            curr_state = outer_self.state

                        if len_queue == 0:
                            outer_self._complete(
                                curr_state=curr_state.get_measured_state(False),
                                prev_state=curr_state.get_measured_state(True),
                            )

                        else:
                            next_index = outer_self.em.next_frame_index(0)
                            outer_self._start_loop(last_ack=last_ack, next_index=next_index)

                    else:
                        outer_self.state = RawBufferedStates.OnErrorOrDownStreamStopped()

            _observe_on(ack, self.scheduler).subscribe(ResultSingle())

        while True:
            next_element = self.queue[0]

            if next_index == 0:

                if isinstance(last_ack, ContinueAck):
                    last_ack = self.underlying.on_next(next_element)

                    with self.lock:
                        self.queue.pop(0)
                        len_queue = len(self.queue)
                        curr_state = self.state

                    if len_queue == 0:
                        self._complete(
                            curr_state=curr_state.get_measured_state(False),
                            prev_state=curr_state.get_measured_state(True),
                        )

                    next_index = self.em.next_frame_index(next_index)

                elif isinstance(last_ack, StopAck):
                    self.state = RawBufferedStates.OnErrorOrDownStreamStopped()
                    return

                else:
                    schedule_ack(last_ack, next=next_element)
                    return

            # schedule next element from time to time
            else:
                schedule_ack(last_ack, next=next_element)
                return

    def on_next(self, elem: ElementType):
        return_ack = continue_ack

        with self.lock:
            len_queue = len(self.queue)
            self.queue.append(elem)

        prev_meas_state = self.state.get_measured_state(bool(len_queue))

        if isinstance(prev_meas_state, BufferedStates.WaitingState):
            last_ack = self.last_ack or prev_meas_state.last_ack

            self._start_loop(last_ack=last_ack, next_index=1)

            return return_ack

        elif isinstance(prev_meas_state, BufferedStates.RunningState):
            return return_ack

        else:
            return stop_ack

    def on_error(self, exc):
        next_raw_state = RawBufferedStates.OnErrorOrDownStreamStopped()

        with self.lock:
            prev_state = self.state
            self.state = next_raw_state

        prev_meas_state = prev_state.get_measured_state(has_elements=False)

        if not isinstance(prev_meas_state, BufferedStates.Completed):
            self.underlying.on_error(exc)

    def _complete(
            self,
            prev_state: BufferedStates.State,
            curr_state: BufferedStates.State,
    ):

        if not isinstance(prev_state, BufferedStates.Completed) and \
                isinstance(curr_state, BufferedStates.Completed):
            self.underlying.on_completed()
            return True

        else:
            return False

    def on_completed(self):
        next_raw_state = RawBufferedStates.OnCompleted(
            prev_state=None,
        )

        with self.lock:
            next_raw_state.prev_state = self.state
            self.state = next_raw_state
            len_queue = len(self.queue)

        self._complete(
            curr_state=next_raw_state.get_measured_state(bool(len_queue)),
            prev_state=next_raw_state.prev_state.get_measured_state(bool(len_queue)),
        )
