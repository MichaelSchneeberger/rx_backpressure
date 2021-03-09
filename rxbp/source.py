import math
from typing import Iterable, Any, List

import rx
from rx import operators
from rx.core import typing

from rxbp.flowable import Flowable
from rxbp.flowables.createflowable import CreateFlowable
from rxbp.flowables.fromemptyflowable import FromEmptyFlowable
from rxbp.flowables.fromiterableflowable import FromIterableFlowable
from rxbp.flowables.fromrxbufferingflowable import FromRxBufferingFlowable
from rxbp.flowables.fromrxevictingflowable import FromRxEvictingFlowable
from rxbp.flowables.fromsingleelementflowable import FromSingleElementFlowable
from rxbp.flowables.intervalflowable import IntervalFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.overflowstrategy import OverflowStrategy, BackPressure, DropOld, ClearBuffer
from rxbp.utils.getstacklines import get_stack_lines


def concat(*sources: Flowable):
    """
    Concatentates Flowables sequences together by back-pressuring the tail Flowables until
    the current Flowable has completed.

    :param sources: Zero or more Flowables
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].concat(*sources[1:])


def empty():
    """
    create a Flowable emitting no elements
    """

    return init_flowable(FromEmptyFlowable())


def from_iterable(iterable: Iterable): #, base: Any = None):
    """
    Create a Flowable that emits each element of the given iterable.

    An iterable cannot be sent in batches.

    :param iterable: the iterable whose elements are sent
    """

    return init_flowable(FromSingleElementFlowable(
        lazy_elem=lambda: iter(iterable),
    ))


def from_list(val: List, batch_size: int = None, base: Any = None):
    """
    Create a Flowable that emits each element of the given list.

    :param val: the list whose elements are sent
    :param batch_size: determines the number of elements that are sent in a batch
    :param base: the base of the Flowable sequence
    """

    buffer = val

    if batch_size is None or len(buffer) == batch_size:

        return init_flowable(FromSingleElementFlowable(
            lazy_elem=lambda: buffer,
        ))

    else:
        if batch_size == 1:
            class EachElementIterable():
                def __iter__(self):
                    return ([e] for e in buffer)

            iterable = EachElementIterable()

        else:
            n_full_slices = int(len(buffer) / batch_size)

            class BatchIterable():
                def __iter__(self):
                    idx = 0
                    for _ in range(n_full_slices):
                        next_idx = idx + batch_size
                        yield buffer[idx:next_idx]
                        idx = next_idx

                    # if there are any elements left
                    if idx < len(buffer):
                        yield buffer[idx:]

            iterable = BatchIterable()

        return init_flowable(FromIterableFlowable(
            iterable=iterable,
        ))


def from_range(arg1: int, arg2: int = None, batch_size: int = None, base: Any = None):
    """
    Create a Flowable that emits elements defined by the range.

    :param arg1: start identifier
    :param arg2: end identifier
    :param batch_size: determines the number of elements that are sent in a batch
    """

    if arg2 is None:
        start_idx = 0
        stop_idx = arg1
    else:
        start_idx = arg1
        stop_idx = arg2

    n_elements = stop_idx - start_idx

    if batch_size is None:
        class FromRangeIterable:
            def __iter__(self):
                return iter(range(start_idx, stop_idx))

        iterable = FromRangeIterable()

        return from_iterable(
            iterable=iterable,
        )

    else:
        n_batches = max(math.ceil(n_elements / batch_size) - 1, 0)

        class FromRangeIterable():
            def __iter__(self):
                current_start_idx = start_idx
                current_stop_idx = start_idx

                for idx in range(n_batches):
                    current_stop_idx = current_stop_idx + batch_size

                    yield range(current_start_idx, current_stop_idx)

                    current_start_idx = current_stop_idx

                yield range(current_start_idx, stop_idx)

        iterable = FromRangeIterable()

        return init_flowable(FromIterableFlowable(
            iterable=iterable,
        ))


def from_rx(
        source: rx.Observable,
        batch_size: int = None,
        overflow_strategy: OverflowStrategy = None,
        is_batched: bool = None,
) -> Flowable:
    """
    Wrap a rx.Observable and exposes it as a Flowable, relaying signals in a backpressure-aware manner.

    :param source: an rx.observable
    :param overflow_strategy: define which batches are ignored once the buffer is full
    :param batch_size: determines the number of elements that are sent in a batch
    :param is_batched: if set to True, the elements emitted by the source rx.Observable are
    either of type List or of type Iterator
    """

    if is_batched is True:
        batched_source = source
    else:
        if batch_size is None:
            batch_size = 1

        batched_source = source.pipe(
            operators.buffer_with_count(batch_size),
        )

    if isinstance(overflow_strategy, DropOld) or isinstance(overflow_strategy, ClearBuffer):
        return init_flowable(FromRxEvictingFlowable(
            batched_source=batched_source,
            overflow_strategy=overflow_strategy,
        ))

    else:
        if overflow_strategy is None:
            buffer_size = math.inf
        elif isinstance(overflow_strategy, BackPressure):
            buffer_size = overflow_strategy.buffer_size
        else:
            raise AssertionError('only BackPressure is currently supported as overflow strategy')

        return init_flowable(FromRxBufferingFlowable(
            batched_source=batched_source,
            overflow_strategy=overflow_strategy,
            buffer_size=buffer_size,
        ))


def merge(*sources: Flowable) -> Flowable:
    """
    Merge the elements of zero or more *Flowables* into a single *Flowable*.

    :param sources: zero or more Flowables whose elements are merged
    """

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].merge(*sources[1:])


def return_value(val: Any):
    """
    Create a Flowable that emits a single element.

    :param val: the single element emitted by the Flowable
    """

    return init_flowable(FromSingleElementFlowable(
        lazy_elem=lambda: [val],
    ))


def zip(*sources: Flowable) -> Flowable: #, result_selector: Callable[..., Any] = None) -> Flowable:
    """
    Create a new Flowable from zero or more Flowables by combining their item in pairs in a strict sequence.

    :param sources: zero or more Flowables whose elements are zipped together
    """

    stack = get_stack_lines()

    if len(sources) == 0:
        return empty()
    else:
        return sources[0].zip(sources[1:], stack=stack)


def create(observer: typing.Subscription) -> Flowable:
    """
    Create a Flowable that emits each element of the given specified subscription function.

    :param observer: Subscription function
    """
    return init_flowable(CreateFlowable(
        observer=observer,
    ))


def interval(period: typing.RelativeTime) -> Flowable:
    """
    Create a Flowable that emits each element after each period.

    :param period: Period for producing the values in the resulting sequence
        (specified as a :class:`float` denoting seconds or an instance of
        :class:`timedelta`).
    """
    return init_flowable(IntervalFlowable(
        period=period,
    ))
