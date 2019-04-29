from rxbp.selectors.selection import SelectCompleted, SelectNext
from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.mergeobservable import merge
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.testing.debugobservable import DebugObservable


def merge_selectors(left: Observable, right: Observable, scheduler: Scheduler):
    obs = ControlledZipObservable(DebugObservable(FilterObservable(left, lambda v: isinstance(v, SelectNext), scheduler=scheduler)), DebugObservable(right),
                                  request_left=lambda l, r: isinstance(r, SelectCompleted),
                                  request_right=lambda l, r: True,
                                  match_func=lambda l, r: isinstance(r, SelectNext),
                                  scheduler=scheduler)
    result = DebugObservable(MapObservable(obs, lambda t2: t2[0]))

    o1 = FilterObservable(left, lambda v: isinstance(v, SelectCompleted), scheduler=scheduler)
    o2 = merge(o1, result)
    o3 = RefCountObservable(source=o2, subject=PublishSubject(scheduler=scheduler))

    return o3


def select_observable(obs: Observable, selector: Observable, scheduler: Scheduler):
    obs = ControlledZipObservable(obs, selector,
                                  request_left=lambda l, r: isinstance(r, SelectCompleted),
                                  request_right=lambda l, r: True,
                                  match_func=lambda l, r: isinstance(r, SelectNext),
                                  scheduler=scheduler)
    result = MapObservable(obs, lambda t2: t2[0])
    return result
