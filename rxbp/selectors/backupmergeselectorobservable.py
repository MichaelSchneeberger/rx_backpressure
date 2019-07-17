import threading
from typing import Callable, Any, Generator, Iterator, Tuple, Optional

from rx.disposable import CompositeDisposable

from rxbp.ack import Ack, continue_ack, stop_ack
from rxbp.selectors.selection import select_next, select_completed, SelectCompleted, SelectNext
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


class MergeSelectorObservable(Observable):
    def __init__(self, left: Observable, right: Observable,
                 scheduler: Scheduler):
        """
        :param left:
        :param right:
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        self.left_observable = left
        self.right_observable = right

        request_left = lambda l, r: isinstance(r, SelectCompleted)
        request_right = lambda l, r: True
        match_func = lambda l, r: isinstance(r, SelectNext)

        self.request_left = request_left
        self.request_right = request_right
        self.match_func = match_func
        self.result_selector = lambda t2: t2[0]

        # self.left_selector = PublishSubject(scheduler=scheduler)
        # self.right_selector = PublishSubject(scheduler=scheduler)

        self.exception = None
        self.left_completed = False
        self.right_completed = False
        self.state = MergeSelectorObservable.WaitLeftRight()

        self.lock = threading.RLock()

    class State:
        pass

    class WaitLeftRight(State):
        pass

    class WaitOnLeft(State):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator,
                     right_in_ack: Ack,
                     right_out_ack: Optional[Ack]
                     ):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack
            self.right_out_ack = right_out_ack

    class WaitOnRight(State):
        def __init__(self,
                     left_iter: Iterator,
                     left_val: Any,
                     left_in_ack: Ack,
                     left_out_ack: Optional[Ack]):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_in_ack = left_in_ack
            self.left_out_ack = left_out_ack

    class Transition(State):
        pass

    class Completed(State):
        pass

    class ConcurrentType:
        pass

    class SynchronousLeft(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, right_in_ack: Ack):
            self.right_in_ack = right_in_ack

    class SynchronousRight(ConcurrentType):
        """ Object indicating if function is called synchronously
        """

        def __init__(self, left_in_ack: Ack):
            self.left_in_ack = left_in_ack

    def observe(self, observer: Observer):
        # exception = [None]
        # left_completed = [False]
        # right_completed = [False]
        #
        # state = [ControlledZipObservable.WaitLeftRight()]
        # lock = threading.RLock()

        def start_zipping(left_val: Any,
                          last_left_out_ack: Optional[Ack], left_iter: Iterator[Tuple[Any, PublishSubject]],
                          right_val: Optional[Any], right_iter: Iterator[Any], last_right_out_ack: Optional[Ack],
                          is_sync: MergeSelectorObservable.ConcurrentType) \
                -> Ack:

            # buffer representing which right elements are selected, and which are not
            left_index_buffer = []
            right_index_buffer = []

            zipped_output_buffer = []

            has_left_elem = True
            has_right_elem = True

            while True:

                # print('left_val={}, right_val={}'.format(left_val, right_val))

                if self.match_func(left_val, right_val):
                    left_index_buffer.append(select_next)
                    right_index_buffer.append(select_next)

                    # add to buffer
                    zipped_output_buffer.append(left_val)

                left_requested = False
                if self.request_left(left_val, right_val):
                    # print('left is lower, right_val_buffer = {}'.format(right_val_buffer[0]))

                    left_index_buffer.append(select_completed)
                    while True:
                        try:
                            new_left_val = next(left_iter)

                            if isinstance(new_left_val, SelectCompleted):
                                zipped_output_buffer.append(new_left_val)
                            else:
                                left_requested = True
                                break
                        except StopIteration:
                            has_left_elem = False
                            break

                if self.request_right(left_val, right_val):
                    # update right index
                    right_index_buffer.append(select_completed)

                    try:
                        right_val = next(right_iter)
                    except StopIteration:
                        has_right_elem = False
                        break

                if not has_left_elem:
                    break

                if left_requested:
                    left_val = new_left_val

            if zipped_output_buffer:
                def gen():
                    yield from zipped_output_buffer

                zip_out_ack = observer.on_next(gen)
            else:
                zip_out_ack = continue_ack

            if left_index_buffer:
                def gen():
                    yield from left_index_buffer

                left_out_ack = self.left_selector.on_next(gen)
            else:
                left_out_ack = last_left_out_ack or continue_ack

            if right_index_buffer:
                def gen():
                    yield from right_index_buffer

                right_out_ack = self.right_selector.on_next(gen)
            else:
                right_out_ack = last_right_out_ack or continue_ack

            if not has_left_elem and not has_right_elem:
                next_state = MergeSelectorObservable.WaitLeftRight()
                self.state = next_state

                result_ack_left = zip_out_ack.merge_ack(left_out_ack)
                result_ack_right = zip_out_ack.merge_ack(right_out_ack)
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    result_ack_right.subscribe(is_sync.right_in_ack)
                    return result_ack_left
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    result_ack_left.subscribe(is_sync.left_in_ack)
                    return result_ack_right
                else:
                    raise Exception('illegal state')

            elif not has_left_elem:
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    right_in_ack = is_sync.right_in_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    right_in_ack = Ack()
                else:
                    raise Exception('illegal state')

                next_state = MergeSelectorObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter, right_in_ack=right_in_ack,
                                                                right_out_ack=right_out_ack)
                self.state = next_state

                result_left_ack = zip_out_ack.merge_ack(left_out_ack)
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    return result_left_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    result_left_ack.subscribe(is_sync.left_in_ack)
                    return right_in_ack
                else:
                    raise Exception('illegal state')

            elif not has_right_elem:
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    left_in_ack = Ack()
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    left_in_ack = is_sync.left_in_ack
                else:
                    raise Exception('illegal state')

                next_state = MergeSelectorObservable.WaitOnRight(left_val=left_val, left_iter=left_iter, left_in_ack=left_in_ack,
                                                                 left_out_ack=left_out_ack)
                self.state = next_state

                result_right_ack = zip_out_ack.merge_ack(right_out_ack)
                if isinstance(is_sync, MergeSelectorObservable.SynchronousLeft):
                    result_right_ack.subscribe(is_sync.right_in_ack)
                    return left_in_ack
                elif isinstance(is_sync, MergeSelectorObservable.SynchronousRight):
                    return result_right_ack
                else:
                    raise Exception('illegal state')

            else:
                raise Exception('illegal case')

        def on_next_left(left_elem: Callable[[], Generator]):
            # print('controlled_zip on left')

            left_iter = left_elem()
            left_val = [next(left_iter)]

            has_elem = [True]
            def gen_first_continue():
                while True:
                    if isinstance(left_val[0], SelectCompleted):
                        yield left_val[0]
                    else:
                        break

                    try:
                        left_val[0] = next(left_iter)
                    except StopIteration:
                        has_elem[0] = False
                        break

            continue_sel = list(gen_first_continue())

            def continue_processing():
                with self.lock:
                    if isinstance(self.state, MergeSelectorObservable.WaitOnLeft):
                        # iterate over right elements and send them over the publish subject and right observer
                        pass

                    elif isinstance(self.state, MergeSelectorObservable.Completed):
                        return stop_ack

                    elif isinstance(self.state, MergeSelectorObservable.WaitLeftRight):
                        # wait until right iterable is received
                        left_in_ack = Ack()
                        new_state = MergeSelectorObservable.WaitOnRight(left_val=left_val[0], left_iter=left_iter,
                                                                        left_in_ack=left_in_ack, left_out_ack=None)
                        self.state = new_state
                        return left_in_ack

                    else:
                        raise Exception('illegal state {}'.format(self.state))

                    # save current state to local variable
                    current_state: MergeSelectorObservable.WaitOnLeft = self.state

                    # change state
                    self.state = MergeSelectorObservable.Transition()

                right_val = current_state.right_val
                right_iter = current_state.right_iter
                right_in_ack = current_state.right_in_ack
                right_out_ack = current_state.right_out_ack

                is_sync = MergeSelectorObservable.SynchronousLeft(right_in_ack=right_in_ack)

                left_ack = start_zipping(
                    left_val=left_val[0],
                    left_iter=left_iter, last_left_out_ack=None,
                    right_val=right_val, right_iter=right_iter,
                    is_sync=is_sync, last_right_out_ack=right_out_ack)

                return left_ack

            if continue_sel:
                def gen():
                    yield from continue_sel

                ack = observer.on_next(gen)

                if has_elem[0]:
                    return_ack = Ack()

                    def _(v):
                        new_ack = continue_processing()
                        new_ack.subscribe(return_ack)

                    ack.subscribe(_)
                    return return_ack
                else:
                    return ack

            else:
                return continue_processing()

        def on_next_right(right_elem: Callable[[], Generator]):
            # print('controlled_zip on right')

            # create the right iterable
            right_iter = right_elem()
            right_val = next(right_iter)

            with self.lock:
                if isinstance(self.state, MergeSelectorObservable.WaitOnRight):
                    pass
                elif isinstance(self.state, MergeSelectorObservable.Completed):
                    return stop_ack
                elif isinstance(self.state, MergeSelectorObservable.WaitLeftRight):
                    right_ack = Ack()
                    new_state = MergeSelectorObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                                                   right_in_ack=right_ack, right_out_ack=None)
                    self.state = new_state
                    return right_ack
                else:
                    raise Exception('illegal state {}'.format(self.state))

                current_state: MergeSelectorObservable.WaitOnRight = self.state
                self.state = MergeSelectorObservable.Transition()

            left_val = current_state.left_val
            left_iter = current_state.left_iter
            left_in_ack = current_state.left_in_ack
            left_out_ack = current_state.left_out_ack

            right_ack = start_zipping(
                left_val=left_val, left_iter=left_iter,
                last_left_out_ack=left_out_ack,
                right_val=right_val, right_iter=right_iter, last_right_out_ack=None,
                is_sync=MergeSelectorObservable.SynchronousRight(left_in_ack=left_in_ack))

            return right_ack

        source = self

        def on_error(exc):
            forward_error = False

            with self.lock:
                if self.exception is not None:
                    return
                elif isinstance(self.state, MergeSelectorObservable.Transition):
                    pass
                else:
                    forward_error = True
                    self.state = MergeSelectorObservable.Completed()

                self.exception = exc

            if forward_error:
                observer.on_error(exc)

        class LeftObserver(Observer):
            def on_next(self, v):
                return on_next_left(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('left completed')
                complete = False

                with source.lock:
                    if source.left_completed:
                        return
                    elif isinstance(source.state, MergeSelectorObservable.WaitLeftRight) or isinstance(source.state, MergeSelectorObservable.WaitOnLeft):
                        complete = True
                        source.state = MergeSelectorObservable.Completed()
                    else:
                        pass

                    source.left_completed = True

                if complete:
                    # source.left_selector.on_completed()
                    # source.right_selector.on_completed()
                    observer.on_completed()

        class RightObserver(Observer):
            def on_next(self, v):
                return on_next_right(v)

            def on_error(self, exc):
                on_error(exc)

            def on_completed(self):
                # print('right on_completed')
                complete = False

                with source.lock:
                    if source.right_completed:
                        return
                    elif isinstance(source.state, MergeSelectorObservable.WaitLeftRight) or isinstance(source.state, MergeSelectorObservable.WaitOnRight):
                        complete = True
                        source.state = MergeSelectorObservable.Completed()
                    else:
                        pass

                    source.right_completed = True

                if complete:
                    # source.left_selector.on_completed()
                    # source.right_selector.on_completed()
                    observer.on_completed()

        left_observer2 = LeftObserver()
        d1 = self.left_observable.observe(left_observer2)

        right_observer2 = RightObserver()
        d2 = self.right_observable.observe(right_observer2)

        return CompositeDisposable(d1, d2)
