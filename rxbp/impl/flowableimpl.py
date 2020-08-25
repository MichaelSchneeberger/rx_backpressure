from dataclasses import replace

from dataclass_abc import dataclass_abc

from rxbp.flowable import Flowable
from rxbp.init.initsharedflowable import init_shared_flowable
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.typing import ValueType


@dataclass_abc
class FlowableImpl(Flowable[ValueType]):
    underlying: FlowableMixin
    # is_hot: bool

    def _copy(self, underlying: FlowableMixin, is_shared: bool = None, *args, **kwargs):
        if is_shared:
            return init_shared_flowable(underlying=underlying)

        return replace(self, underlying=underlying, *args, **kwargs)
