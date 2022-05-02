from typing import Any


class CircularList(object):
    def __init__(self, slots: int) -> None:
        if not slots:
            raise ValueError
        self.list = [None for _ in range(slots)]
        self.index = 0
        self.max_index = len(self.list) - 1

    def next(self) -> Any:
        if self.index == self.max_index:
            self.index = 0
        else:
            self.index += 1
        return self.list[self.index]

    def current(self, value: Any) -> None:
        self.list[self.index] = value
        return


class Barrier(object):
    pass
