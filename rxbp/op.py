from typing import Any, Callable

from rxbp.observable import Observable
from rxbp.observableoperator import ObservableOperator
from rxbp.observables.connectableobservable import ConnectableObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.flatmapobservablemulti import FlatMapObservableMulti
from rxbp.observables.flatzipobservablemulti import FlatZipObservableMulti
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.observeonobservable import ObserveOnObservable
from rxbp.observables.pairwiseobservable import PairwiseObservable
from rxbp.observables.repeatfirstobservable import RepeatFirstObservable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observables.zipwithindexobservable import ZipWithIndexObservable
from rxbp.observers.bufferedsubscriber import BufferedSubscriber
from rxbp.scheduler import Scheduler
from rxbp.subjects.cachedservefirstsubject import CachedServeFirstSubject
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.subjects.replaysubject import ReplaySubject
from rxbp.testing.debugobservable import DebugObservable


def buffer(size: int):
    def func(obs: Observable):
        class ToBackpressureObservable(Observable):
            def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
                subscriber = BufferedSubscriber(observer=observer, scheduler=scheduler, buffer_size=size)
                disposable = obs.unsafe_subscribe(subscriber, scheduler, subscribe_scheduler)
                return disposable
        return ToBackpressureObservable()
    return ObservableOperator(func)


def cache():
    """ Converts this observable into a multicast observable that caches the items that the fastest observer has
    already received and the slowest observer has not yet requested. Note that this observable is subscribed when
    the multicast observable is subscribed for the first time. Therefore, this observable is never subscribed more
    than once.

    :return: multicast observable
    """

    def func(obs: Observable):
        return ConnectableObservable(source=obs, subject=CachedServeFirstSubject()).ref_count()
    return ObservableOperator(func)


def debug(name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None):
    def func(obs: Observable):
        return DebugObservable(source=obs, name=name, on_next=on_next, on_subscribe=on_subscribe, on_ack=on_ack,
                                 on_raw_ack=on_raw_ack)
    return ObservableOperator(func)


def execute_on(scheduler: Scheduler):
    def func(obs: Observable):
        class ExecuteOnObservable(Observable):
            def unsafe_subscribe(self, observer, _, subscribe_scheduler):
                disposable = obs.unsafe_subscribe(observer, scheduler, subscribe_scheduler)
                return disposable

        return ExecuteOnObservable()
    return ObservableOperator(func)


def filter(predicate: Callable[[Any], bool]):
    """ Only emits those items for which the given predicate holds

    :param predicate: a function that evaluates the items emitted by the source returning True if they pass the
    filter
    :return: filtered observable
    """

    def func(obs: Observable):
        return FilterObservable(source=obs, predicate=predicate)
    return ObservableOperator(func)


def flat_map(selector: Callable[[Any], Observable]):
    """ Applies a function to each item emitted by the source and flattens the result. The function takes any type
    as input and returns an inner observable. The resulting observable concatenates the items of each inner
    observable.

    :param selector: A function that takes any type as input and returns an observable.
    :return: a flattened observable
    """

    def func(obs: Observable):
        return FlatMapObservableMulti(source=obs, selector=selector)
    return ObservableOperator(func)


def flat_zip(right: Observable, inner_selector: Callable[[Any], Observable], left_selector: Callable[[Any], Any]=None,
             result_selector: Callable[[Any, Any, Any], Any] = None):
    def func(obs: Observable):
        return FlatZipObservableMulti(left=obs, right=right,
                                      inner_selector=inner_selector, left_selector=left_selector,
                                      result_selector=result_selector)
    return ObservableOperator(func)


def map(selector: Callable[[Any], Any]):
    """ Maps each item emitted by the source by applying the given function

    :param selector: function that defines the mapping
    :return: mapped observable
    """

    def func(obs: Observable):
        return MapObservable(source=obs, selector=selector)
    return ObservableOperator(func)


def observe_on(scheduler: Scheduler):
    """ Operator that specifies a specific scheduler, on which observers will observe events

    :param scheduler: a rxbackpressure scheduler
    :return: an observable running on specified scheduler
    """

    def func(obs: Observable):
        return ObserveOnObservable(source=obs, scheduler=scheduler)
    return ObservableOperator(func)


def pairwise():
    """ Creates an observable that pairs each neighbouring two items from the source

    :param selector: (optional) selector function
    :return: paired observable
    """

    def func(obs: Observable):
        return PairwiseObservable(source=obs)
    return ObservableOperator(func)


def repeat_first():
    """ Repeat the first item forever

    :return:
    """

    def func(obs: Observable):
        return RepeatFirstObservable(source=obs)
    return ObservableOperator(func)


def replay():
    """ Converts this observable into a multicast observable that replays the item received by the source. Note
    that this observable is subscribed when the multicast observable is subscribed for the first time. Therefore,
    this observable is never subscribed more than once.

    :return: multicast observable
    """

    def func(obs: Observable):
        observable = ConnectableObservable(
            source=obs,
            subject=ReplaySubject()
        ).ref_count()
        return observable
    return ObservableOperator(func)


def share():
    """ Converts this observable into a multicast observable that backpressures only after each subscribed
    observer backpressures. Note that this observable is subscribed when the multicast observable is subscribed for
    the first time. Therefore, this observable is never subscribed more than once.

    :return: multicast observable
    """

    def func(obs: Observable):
        return ConnectableObservable(source=obs, subject=PublishSubject()).ref_count()
    return ObservableOperator(func)


def zip(right: Observable):
    """ Creates a new observable from two observables by combining their item in pairs in a strict sequence.

    :param selector: a mapping function applied over the generated pairs
    :return: zipped observable
    """

    def func(obs: Observable):
        return Zip2Observable(left=obs, right=right)
    return ObservableOperator(func)


def zip_with_index(selector: Callable[[Any, int], Any] = None):
    """ Zips each item emmited by the source with their indices

    :param selector: a mapping function applied over the generated pairs
    :return: zipped with index observable
    """

    def func(obs: Observable):
        return ZipWithIndexObservable(source=obs, selector=selector)
    return ObservableOperator(func)