import functools
import itertools
from abc import abstractmethod, ABC
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, Tuple, Iterator, List

import rx

from rxbp.acknowledgement.ack import Ack
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.flowables.defaultifemptyflowable import DefaultIfEmptyFlowable
from rxbp.flowables.doactionflowable import DoActionFlowable
from rxbp.flowables.evictingbufferflowable import EvictingBufferFlowable
from rxbp.flowables.filterflowable import FilterFlowable
from rxbp.flowables.firstflowable import FirstFlowable
from rxbp.flowables.firstordefaultflowable import FirstOrDefaultFlowable
from rxbp.flowables.flatmapflowable import FlatMapFlowable
from rxbp.flowables.init.initdebugflowable import init_debug_flowable
from rxbp.flowables.lastflowable import LastFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.maptoiteratorflowable import MapToIteratorFlowable
from rxbp.flowables.mergeflowable import MergeFlowable
from rxbp.flowables.observeonflowable import ObserveOnFlowable
from rxbp.flowables.pairwiseflowable import PairwiseFlowable
from rxbp.flowables.reduceflowable import ReduceFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.flowables.repeatfirstflowable import RepeatFirstFlowable
from rxbp.flowables.scanflowable import ScanFlowable
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.flowables.tolistflowable import ToListFlowable
from rxbp.flowables.zipflowable import ZipFlowable
from rxbp.flowables.zipwithindexflowable import ZipWithIndexFlowable
from rxbp.mixins.flowableabsopmixin import FlowableAbsOpMixin
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.mixins.sharedflowablemixin import SharedFlowableMixin
from rxbp.observables.materializeobservable import MaterializeObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.overflowstrategy import OverflowStrategy
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.torx import to_rx
from rxbp.typing import ValueType
from rxbp.utils.getstacklines import get_stack_lines


class FlowableOpMixin(
    FlowableAbsOpMixin,
    FlowableMixin,
    ABC,
):
    @property
    @abstractmethod
    def underlying(self) -> FlowableMixin:
        ...

    @abstractmethod
    def _copy(
            self,
            **kwargs,
    ) -> 'FlowableOpMixin':
        ...

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        return self.underlying.unsafe_subscribe(subscriber=subscriber)

    # def pipe(self, *operators: PipeOperation['FlowableOpMixin']) -> 'FlowableOpMixin':
    #     raw = functools.reduce(lambda obs, op: op(obs), operators, self)
    #     return self._copy(raw)

    def buffer(self, buffer_size: int = None) -> 'FlowableOpMixin':
        flowable = BufferFlowable(source=self, buffer_size=buffer_size)
        return self._copy(underlying=flowable)

    def concat(self, *others: FlowableMixin) -> 'FlowableOpMixin':
        if len(others) == 0:
            return self

        all_sources = itertools.chain([self], others)
        flowable = ConcatFlowable(sources=list(all_sources))

        sources = (self,) + others

        try:
            source = next(source for source in sources if isinstance(source, SharedFlowableMixin))
        except StopIteration:
            source = self

        return source._copy(underlying=flowable)

    def controlled_zip(
            self,
            right: FlowableMixin,
            stack: List[FrameSummary],
            request_left: Callable[[Any, Any], bool] = None,
            request_right: Callable[[Any, Any], bool] = None,
            match_func: Callable[[Any, Any], bool] = None,
    ) -> 'FlowableOpMixin':

        assert isinstance(right, FlowableMixin), f'"{right}" must be of type FlowableMixin'

        flowable = ControlledZipFlowable(
            left=self,
            right=right,
            stack=stack,
            request_left=request_left,
            request_right=request_right,
            match_func=match_func,
        )

        try:
            source = next(source for source in [self, right] if isinstance(source, SharedFlowableMixin))
        except StopIteration:
            source = self

        return source._copy(underlying=flowable)

    def debug(
            self,
            name: str,
            on_next: Callable[[Any], Ack],
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_sync_ack: Callable[[Ack], None] = None,
            on_async_ack: Callable[[Ack], None] = None,
            on_observe: Callable[[ObserverInfo], None] = None,
            on_subscribe: Callable[[Subscriber], None] = None,
            on_raw_ack: Callable[[Ack], None] = None,
            stack: List[FrameSummary] = None,
            verbose: bool = None
    ):

        return self._copy(underlying=init_debug_flowable(
            source=self,
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
        ))

    def default_if_empty(self, lazy_val: Callable[[], Any]) -> 'FlowableOpMixin':
        return self._copy(
            underlying=DefaultIfEmptyFlowable(source=self, lazy_val=lazy_val),
        )

    def do_action(
            self,
            on_next: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            on_error: Callable[[Exception], None] = None,
            on_disposed: Callable[[], None] = None,
    ) -> 'FlowableOpMixin':
        return self._copy(underlying=DoActionFlowable(
            source=self,
            on_next=on_next,
            on_completed=on_completed,
            on_error=on_error,
            on_disposed=on_disposed,
        ))

    # def execute_on(self, scheduler: Scheduler):
    #     return self._copy(underlying=ExecuteOnFlowable(source=self, scheduler=scheduler))

    def filter(
            self,
            predicate: Callable[[Any], bool],
            stack: List[FrameSummary],
    ) -> 'FlowableOpMixin':
        flowable = FilterFlowable(source=self, predicate=predicate)
        return self._copy(underlying=flowable)

    def first(self, stack: List[FrameSummary]):
        flowable = FirstFlowable(source=self, stack=stack)
        return self._copy(underlying=flowable)

    def first_or_default(self, lazy_val: Callable[[], Any]):
        flowable = FirstOrDefaultFlowable(source=self, lazy_val=lazy_val)
        return self._copy(underlying=flowable)

    def flat_map(self, func: Callable[[Any], 'FlowableOpMixin'], stack: List[FrameSummary]):
        flowable = FlatMapFlowable(source=self, func=func, stack=stack)
        return self._copy(underlying=flowable)

    def last(self, stack: List[FrameSummary]):
        flowable = LastFlowable(source=self, stack=stack)
        return self._copy(underlying=flowable)

    def map(self, func: Callable[[ValueType], Any]):
        flowable = MapFlowable(source=self, func=func)
        return self._copy(underlying=flowable)

    def map_to_iterator(
            self,
            func: Callable[[ValueType], Iterator[ValueType]],
    ):
        flowable = MapToIteratorFlowable(source=self, func=func)
        return self._copy(underlying=flowable)

    def materialize(
            self,
    ):
        @dataclass
        class MaterializeFlowable(FlowableMixin):
            source: FlowableMixin

            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                subscription = self.source.unsafe_subscribe(subscriber=subscriber)
                return subscription.copy(
                    observable=MaterializeObservable(
                        source=subscription.observable,
                    )
                )

        return self._copy(underlying=MaterializeFlowable(source=self))

    def merge(self, *others: FlowableMixin):

        assert all(isinstance(source, FlowableMixin) for source in others), \
            f'"{others}" must all be of type FlowableMixin'

        if len(others) == 0:
            return self
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def func(right: 'FlowableOpMixin' = None, left: 'FlowableOpMixin' = source):
                        if right is None:
                            return left
                        else:
                            flowable = MergeFlowable(source=left, other=right)
                            return flowable

                    yield func

            flowable: FlowableMixin = functools.reduce(lambda acc, func: func(acc), gen_stack(), None)

            try:
                source = next(source for source in sources if isinstance(source, SharedFlowableMixin))
            except StopIteration:
                source = self

            return source._copy(underlying=flowable)

    def observe_on(self, scheduler: Scheduler):

        return self._copy(underlying=ObserveOnFlowable(source=self, scheduler=scheduler))

    def pairwise(self) -> 'FlowableOpMixin':

        return self._copy(underlying=PairwiseFlowable(source=self))

    def reduce(
            self,
            func: Callable[[Any, Any], Any],
            initial: Any,
    ):
        flowable = ReduceFlowable(
            source=self,
            func=func,
            initial=initial,
        )
        return self._copy(underlying=flowable)

    def repeat_first(self):

        flowable = RepeatFirstFlowable(source=self)
        return self._copy(underlying=flowable)

    def scan(self, func: Callable[[Any, Any], Any], initial: Any):
        flowable = ScanFlowable(source=self, func=func, initial=initial)
        return self._copy(underlying=flowable)

    def _share(self, stack: List[FrameSummary]):
        return self._copy(underlying=RefCountFlowable(source=self, stack=stack), is_shared=True)

    def to_list(self):

        flowable = ToListFlowable(source=self)
        return self._copy(underlying=flowable)

    def to_rx(self, batched: bool = None) -> rx.Observable:
        """ Converts this Flowable to an rx.Observable

        :param batched: if True, then the elements emitted by the Observable are expected to
        be a list or an iterator.
        """

        return to_rx(source=self, batched=batched)

    def zip(self, others: Tuple['FlowableOpMixin'], stack: List[FrameSummary]):

        assert all(isinstance(source, FlowableMixin) for source in others), \
            f'"{others}" must all be of type FlowableMixin'

        if len(others) == 0:
            return self.map(lambda v: (v,))
        else:
            sources = (self,) + others

            def gen_stack():
                for source in reversed(sources):
                    def func(right: 'FlowableOpMixin' = None, left: 'FlowableOpMixin' = source):
                        if right is None:
                            return left.map(lambda v: (v,))
                        else:
                            def inner_result_selector(t: Tuple[Any, Tuple[Any]]):
                                return (t[0],) + t[1]

                            flowable = MapFlowable(
                                source=ZipFlowable(
                                    left=left,
                                    right=right,
                                    stack=stack,
                                ),
                                func=inner_result_selector,
                            )
                            return flowable

                    yield func

            flowable: FlowableMixin = functools.reduce(lambda acc, func: func(acc), gen_stack(), None)

            try:
                source = next(source for source in sources if isinstance(source, SharedFlowableMixin))
            except StopIteration:
                source = self

            return source._copy(underlying=flowable)

    def zip_with_index(self, selector: Callable[[Any, int], Any] = None):

        flowable = ZipWithIndexFlowable(source=self, selector=selector)
        return self._copy(underlying=flowable)

    def strategy(self, overflow_strategy: OverflowStrategy) -> 'FlowableOpMixin':
        flowable = EvictingBufferFlowable(source=self, overflow_strategy=overflow_strategy)
        return self._copy(underlying=flowable)

    def subscribe_on(self, scheduler: Scheduler):
        return self._copy(underlying=SubscribeOnFlowable(source=self, scheduler=scheduler))
