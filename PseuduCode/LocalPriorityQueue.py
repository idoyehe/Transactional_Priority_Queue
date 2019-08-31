from __future__ import annotations
from PQNode import Turn, PQNode


class LocalPriorityQueue(object):
    def __init__(self):
        self.root: PQNode = None
        self.size: int = 0
        self.version = None  # TODO: figure out if that really needed? because it is local PQ

    def top(self) -> object:
        if self.root is not None:
            return self.root.value
        raise Exception("The priority queue is empty")

    def _search_node_(self, index: int) -> PQNode:
        if index is None or index <= 0 or index > self.size:
            raise Exception("Provided index is invalid")

        binary_digits = list(bin(index)[3:])

        return self.root.search(index, binary_digits, 0)

    def pop(self) -> object:
        if self.root is None:
            raise Exception("The priority queue is empty")

        min_node_value = self.root.value

        if self.size == 1:
            self.root = None
            self.size -= 1
            return min_node_value

        swapper: PQNode = self._search_node_(self.size)
        swapper.swap(self.root)

        if self.size % 2 == Turn.LEFT.value:
            swapper.father.left = None
        else:
            swapper.father.right = None

        self.size -= 1
        self.root.sift_down()
        return min_node_value

    def push(self, value: object, priority: int, sim_index=None) -> None:
        new_node = PQNode(value=value, priority=priority, index=self.size + 1,
                          sim_index=sim_index)  # TODO: what version new node is getting?

        if self.root is None:
            self.root = new_node
            self.size += 1
            return

        new_node_father = self._search_node_(int((self.size + 1) / 2))
        if (self.size + 1) % 2 == Turn.LEFT.value:
            new_node_father.left = new_node
        else:
            new_node_father.right = new_node

        new_node.father = new_node_father
        self.size += 1
        new_node.sift_up()
        return

    def k_th_smallest(self, k: int) -> object:
        def _left(index) -> int:
            return 2 * index

        def _right(index) -> int:
            return 2 * index + 1

        pq: LocalPriorityQueue = LocalPriorityQueue()  # Create a Local Priority Queue

        pq.push(value=self.root.value, priority=self.root.priority, sim_index=self.root.index)

        for i in range(1, k):
            top_index: int = pq.root.sim_index
            pq.pop()
            left_son_index: int = _left(top_index)
            right_son_index: int = _right(top_index)
            if left_son_index <= self.size:
                left_son: PQNode = self._search_node_(left_son_index)
                pq.push(value=left_son.value, priority=left_son.priority, sim_index=left_son.index)

            if right_son_index <= self.size:
                right_son: PQNode = self._search_node_(right_son_index)
                pq.push(value=right_son.value, priority=right_son.priority, sim_index=right_son.index)

        return pq.top()
