from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


class AnonymousSubFlowObservable(SubFlowObservable):
    """Class to create an Observable instance from a delegate-based
    implementation of the Subscribe method."""

    def __init__(self, subscribe_func):
        """Creates an observable sequence object from the specified
        subscription function.

        Keyword arguments:
        :param types.FunctionType subscribe: Subscribe method implementation.
        """

        self._subscribe_func = subscribe_func
        super().__init__()

    def _subscribe_core(self, observer, scheduler=None):
        return self._subscribe_func(observer, scheduler)