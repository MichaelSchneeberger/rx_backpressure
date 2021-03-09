from typing import Any, Callable, Iterator

from rxbp.acknowledgement.ack import Ack
from rxbp.flowable import Flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.observerinfo import ObserverInfo
from rxbp.overflowstrategy import OverflowStrategy
from rxbp.pipeoperation import PipeOperation
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


def buffer(buffer_size: int = None):
    """
    Buffer the element emitted by the source without back-pressure until the buffer is full.
    """

    def op_func(source: Flowable):
        return source.buffer(buffer_size=buffer_size)

    return PipeOperation(op_func)


def concat(*sources: FlowableMixin):
    """
    Concatentates Flowables sequences together by back-pressuring the tail Flowables until
    the current Flowable has completed.

    :param sources: other Flowables that get concatenate to this Flowable.
    """

    def op_func(left: Flowable):
        return left.concat(*sources)

    return PipeOperation(op_func)


def controlled_zip(
        right: FlowableMixin,
        request_left: Callable[[Any, Any], bool] = None,
        request_right: Callable[[Any, Any], bool] = None,
        match_func: Callable[[Any, Any], bool] = None,
):
    """
    Creates a new Flowable from two Flowables by combining their elements in pairs. Which
    element gets paired with an element from the other Flowable is determined by two functions
    called `request_left` and `request_right`.

    :param right: other Flowable
    :param request_left: a function that returns True, if a new element from the left \
    Flowable is requested to build the the next pair
    :param request_right: a function that returns True, if a new element from the left \
    Flowable is requested to build the the next pair
    :param match_func: a filter function that returns True, if the current pair is sent \
    downstream
    :return: zipped Flowable
    """

    stack = get_stack_lines()

    def op_func(left: Flowable):
        return left.controlled_zip(
            right=right,
            stack=stack,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )

    return PipeOperation(op_func)


def debug(
        name: str,
        on_next: Callable[[Any], Ack] = None,
        on_completed: Callable[[], None] = None,
        on_error: Callable[[Exception], None] = None,
        on_sync_ack: Callable[[Ack], None] = None,
        on_async_ack: Callable[[Ack], None] = None,
        on_observe: Callable[[ObserverInfo], None] = None,
        on_subscribe: Callable[[Subscriber], None] = None,
        on_raw_ack: Callable[[Ack], None] = None,
        verbose: bool = None,
):
    """
    Print debug messages to the console when providing the `name` argument

    :on_next: customize the on next debug console print
    """

    stack = get_stack_lines()

    def op_func(source: Flowable):
        return source.debug(
            name=name,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_observe=on_observe,
            on_subscribe=on_subscribe,
            on_sync_ack=on_sync_ack,
            on_async_ack=on_async_ack,
            on_raw_ack=on_raw_ack,
            stack=stack,
            verbose=verbose,
        )

    return PipeOperation(op_func)


def default_if_empty(lazy_val: Callable[[], Any]):
    """
    Only the elements of the source or a default value if the source is an empty sequence

    :param lazy_val: a function that returns the default value
    """

    def op_func(left: Flowable):
        return left.default_if_empty(lazy_val=lazy_val)

    return PipeOperation(op_func)


def do_action(
        on_next: Callable[[Any], None] = None,
        on_completed: Callable[[], None] = None,
        on_error: Callable[[Exception], None] = None,
        on_disposed: Callable[[], None] = None,
):
    def op_func(left: Flowable):
        return left.do_action(
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        )

    return PipeOperation(op_func)


# def execute_on(scheduler: Scheduler):
#     """
#     Inject new scheduler that is used to subscribe the Flowable.
#     """
#
#     def op_func(left: Flowable):
#         return left.execute_on(scheduler=scheduler)
#
#     return PipeOperation(op_func)


def fast_filter(predicate: Callable[[Any], bool]):
    """
    Only emit those elements for which the given predicate holds.

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    def op_func(left: Flowable):
        return left.fast_filter(predicate=predicate)

    return PipeOperation(op_func)


def filter(predicate: Callable[[Any], bool]):
    """
    Only emit those elements for which the given predicate holds.

    :param predicate: a function that returns True, if the current element passes the filter
    :return: filtered Flowable
    """

    stack = get_stack_lines()

    def op_func(left: Flowable):
        return left.filter(predicate=predicate, stack=stack)

    return PipeOperation(op_func)


def first():
    """
    Emit the first element only and stop the Flowable sequence thereafter.

    The `raise_exception` argument needs to be given as follows:

    ::

        rxbp.op.first(raise_exception=lambda f: f())

    The `raise_exception` argument is called in case there is no first element.
    Like RxPY, a SequenceContainsNoElementsError is risen. But on top of that
    the place where the `first` argument is used is added to the traceback by
    providing the `raise_exception` argument.
    """

    stack = get_stack_lines()

    def op_func(source: Flowable):
        return source.first(stack=stack)

    return PipeOperation(op_func)


def first_or_default(lazy_val: Callable[[], Any]):
    """
    Emit the first element only and stop the Flowable sequence thereafter.
    """

    def op_func(source: Flowable):
        return source.first_or_default(lazy_val=lazy_val)

    return PipeOperation(op_func)


def flat_map(func: Callable[[Any], Flowable]):
    """
    Apply a function to each item emitted by the source and flattens the result.

    The specified function must return a Flowable. The resulting Flowable
    concatenates the elements of each inner Flowables.
    The resulting Flowable concatenates the items of each inner Flowable.
    """

    stack = get_stack_lines()

    def op_func(source: Flowable):
        return source.flat_map(func=func, stack=stack)

    return PipeOperation(op_func)


def flatten():
    stack = get_stack_lines()

    def op_func(source: Flowable):
        return source.flat_map(func=lambda v: v, stack=stack)

    return PipeOperation(op_func)


def last():
    """
    Emit the last element of the Flowable sequence

    The `raise_exception` argument needs to be given as follows:

    ::

        rxbp.op.last(raise_exception=lambda f: f())

    The `raise_exception` argument is called in case there is no element.
    Like RxPY, a SequenceContainsNoElementsError is risen. But on top of that
    the place where the `last` argument is used is added to the traceback by
    providing the `raise_exception` argument.
    """

    stack = get_stack_lines()

    def op_func(source: Flowable):
        return source.last(stack=stack)

    return PipeOperation(op_func)


def map(func: Callable[[Any], Any]):
    """ Map each element emitted by the source by applying the given function.

    :param func: function that defines the mapping applied to each element of the \
    Flowable sequence.
    """

    def op_func(source: Flowable):
        return source.map(func=func)

    return PipeOperation(op_func)


def map_to_iterator(
        func: Callable[[ValueType], Iterator[ValueType]],
):
    """
    Create a Flowable that maps each element emitted by the source to an iterator
    and emits each element of these iterators.

    :param func: function that defines the mapping applied to each element to an iterator.
    """

    def op_func(source: Flowable):
        return source.map_to_iterator(func=func)

    return PipeOperation(op_func)


def merge(*others: Flowable):
    """
    Merge the elements of this and the other Flowable sequences into a single *Flowable*.

    :param sources: other Flowables that get merged to this Flowable.
    """

    def op_func(left: Flowable):
        return left.merge(*others)

    return PipeOperation(op_func)


def observe_on(scheduler: Scheduler):
    """
    Schedule elements emitted by the source on a dedicated scheduler.

    :param scheduler: a rxbackpressure scheduler
    :return: an Flowable running on specified scheduler
    """

    def op_func(source: Flowable):
        return source.observe_on(scheduler=scheduler)

    return PipeOperation(op_func)


def pairwise():
    """
    Create a Flowable that emits a pair for each consecutive pairs of elements
    in the Flowable sequence.
    """

    def op_func(source: Flowable):
        return source.pairwise()

    return PipeOperation(op_func)


def reduce(
        func: Callable[[Any, Any], Any],
        initial: Any,
):
    """
    Apply an accumulator function over a Flowable sequence and emits a single element.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a Flowable that emits the final accumulated value
    """

    def op_func(source: Flowable):
        return source.reduce(func=func, initial=initial)

    return PipeOperation(op_func)


def repeat_first():
    """
    Return a Flowable that repeats the first element it receives from the source
    forever (until disposed).
    """

    def op_func(source: Flowable):
        return source.repeat_first()

    return PipeOperation(op_func)


def scan(func: Callable[[Any, Any], Any], initial: Any):
    """
    Apply an accumulator function over a Flowable sequence and return each intermediate result.

    The initial value is used as the initial accumulator value.

    :param func: An accumulator function to be invoked on each element
    :param initial: The initial accumulator value
    :return: a Flowable that emits the accumulated values
    """

    def op_func(source: Flowable):
        return source.scan(func=func, initial=initial)

    return PipeOperation(op_func)


# def share():
#     """
#     Broadcast the elements of the Flowable to possibly multiple subscribers.
#
#     This function is only valid when used inside a Multicast. Otherwise, it
#     raise an exception.
#     """
#
#     def inner_func(source: Flowable):
#         return source.share()#bind_to=ability)
#
#     return PipeOperation(inner_func)


def to_list():
    """
    Create a new Flowable that collects the elements from the source sequence,
    and emits a single element of type List.
    """

    def op_func(source: Flowable):
        return source.to_list()

    return PipeOperation(op_func)


def zip(*others: Flowable):
    """
    Create a new Flowable from one or more Flowables by combining their item in pairs in a strict sequence.

    :param others: :param sources: other Flowables that get zipped to this Flowable.
    """

    stack = get_stack_lines()

    def op_func(left: Flowable):
        return left.zip(others=others, stack=stack)

    return PipeOperation(op_func)


def zip_with_index():
    """
    Zip each item emitted by the source with the enumerated index.
    """

    def op_func(left: Flowable):
        return left.zip_with_index()

    return PipeOperation(op_func)


def strategy(overflow_strategy: OverflowStrategy):
    """
    Buffer the element emitted by the source use overflow strategy.
    """

    def op_func(source: Flowable):
        return source.strategy(overflow_strategy=overflow_strategy)

    return PipeOperation(op_func)


def subscribe_on(scheduler: Scheduler):
    """
    Subscribe on the specified scheduler.

    This only performs the side-effects of subscription and
    unsubscription on the specified scheduler. In order to invoke
    observer callbacks on a scheduler, use observe_on.

    :param scheduler: a rxbackpressure scheduler
    """

    def op_func(source: Flowable):
        return source.subscribe_on(scheduler=scheduler)

    return PipeOperation(op_func)
