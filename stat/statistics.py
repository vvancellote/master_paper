#
# Statistics Class
#
from typing import List


class StatisticsVariable(object):
    def __init__(self) -> None:
        self.first_momentum: List = list()
        self.second_momentum: List = list()
        self.length: List = list()
        self.description: List = list()
        self.size: int = 0

    def create_variable(self, description: str = None) -> int:
        self.first_momentum.append(0.0)
        self.second_momentum.append(0.0)
        self.length.append(0)
        self.description.append(description)
        id = self.size
        self.size += 1
        return id

    def add_value(self, id: int, value: float) -> None:
        delta = value - self.first_momentum[id]
        self.length[id] += 1
        self.first_momentum[id] += delta / self.length[id]
        delta2 = value - self.first_momentum[id]
        self.second_momentum[id] += delta * delta2
        return

    def bulk_add_value(self, id: int, value_list: list) -> None:
        for i in value_list:
            self.add_value(id, i)
        return

    def mean(self, id: int) -> float:
        return self.first_momentum[id]

    def variance(self, id: int) -> float:
        return self.second_momentum[id]

    def length(self, id: int) -> int:
        return self.length[id]

    def size(self, id: int) -> int:
        return self.size

    def dump(self, file_name: str) -> None:
        with open(file_name, "w") as fd:
            fd.write("ID,MEAN,VARIANCE,NUMOBS,DESCRIPTION\n")
            for i, desc in enumerate(self.description):
                m = self.first_momentum[i]
                v = self.second_momentum[i]
                l = self.length[i]
                fd.write(f"{i},{m},{v},{l},{desc}\n")
        return
