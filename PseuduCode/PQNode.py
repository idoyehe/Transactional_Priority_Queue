from __future__ import annotations

from enum import Enum


class Turn(Enum):
    LEFT = 0
    RIGHT = 1


class PQNode(object):
    def __init__(self, value: object, priority: int, index: int, left=None, right=None, father=None, nodeVersion=None,sim_index=None):
        self.index: int = index  # TODO: figure out if that really needed?
        self.priority: int = priority
        self.value: object = value
        self.left: PQNode = left  # left son heap
        self.right: PQNode = right  # right son heap
        self.father: PQNode = father  # father heap
        self.nodeVersion = nodeVersion  # current node version (vc)
        self.sim_index = sim_index

    def search(self, index: int, binary_digits: list, current_turn_index: int) -> PQNode:

        if index == self.index:
            return self

        if current_turn_index < 0:
            raise Exception(f"The current index: {index} is not found")

        if int(binary_digits[current_turn_index]) == Turn.LEFT.value:
            return self.left.search(index, binary_digits, current_turn_index + 1)

        return self.right.search(index, binary_digits, current_turn_index + 1)

    def swap(self, node: PQNode) -> None:  # TODO: update version or swap version?
        temp_value = self.value
        temp_priority = self.priority
        temp_sim_index = self.sim_index
        self.value = node.value
        self.priority = node.priority
        self.sim_index = node.sim_index
        node.value = temp_value
        node.priority = temp_priority
        node.sim_index = temp_sim_index

    def sift_up(self) -> None:
        if self.father is None or self.father.priority <= self.priority:  # in case no need to sift up
            return

        self.swap(self.father)
        self.father.sift_up()

    def sift_down(self) -> None:
        if self.left is None and self.right is None:  # in case no more down steps
            return

        min_son: PQNode = None
        if self.left is not None and self.priority > self.left.priority:
            min_son = self.left

        if self.right is not None and self.priority > self.right.priority:
            if min_son is None or self.right.priority < min_son.priority:
                min_son = self.right
        if min_son:
            self.swap(min_son)
            return min_son.sift_down()
        return

