from abc import ABC


class OverflowStrategy(ABC):
    def __init__(self, buffer_size: int):
        self.buffer_size = buffer_size


class BackPressure(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass


class DropOld(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass


class ClearBuffer(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass


class Drop(OverflowStrategy):
    """
    Drop strategy will perform opposite of DropOld and will refuse new messages as long as the buffer is full
    and will accept them only once the buffer has available space.
    """
    pass
