import pytest
from LocalPriorityQueue import *


def test_local_priority_queue_constructor():
    lpq = LocalPriorityQueue()


def test_first_insertion():
    lpq = LocalPriorityQueue()
    lpq.push(1, 1)


def test_first_top():
    lpq = LocalPriorityQueue()
    lpq.push(1, 1)
    assert lpq.top() == 1
    assert lpq.size == 1


def test_pop():
    lpq = LocalPriorityQueue()
    lpq.push(1, 1)
    lpq.push(2, 2)
    lpq.push(3, 3)
    assert lpq.top() == 1
    assert lpq.size == 3
    assert lpq.root.left.value == 2
    assert lpq.root.right.value == 3
    lpq.pop()
    assert lpq.top() == 2
    assert lpq.size == 2
    lpq.pop()
    assert lpq.top() == 3
    assert lpq.size == 1
    lpq.pop()
    assert lpq.size == 0
    with pytest.raises(Exception):
        lpq.top()


def test_pop():
    lpq = LocalPriorityQueue()
    lpq.push(3, 3)
    lpq.push(2, 2)
    lpq.push(1, 1)
    assert lpq.top() == 1
    assert lpq.size == 3
    assert lpq.root.left.value == 3
    assert lpq.root.right.value == 2
    lpq.pop()
    assert lpq.top() == 2
    assert lpq.size == 2
    lpq.pop()
    assert lpq.top() == 3
    assert lpq.size == 1
    lpq.pop()
    assert lpq.size == 0
    with pytest.raises(Exception):
        lpq.top()


def heap_invariant(node: PQNode):
    if node is None:
        return
    if node.father:
        assert node.priority > node.father.priority
    heap_invariant(node.left)
    heap_invariant(node.right)


def test_overall():
    lpq = LocalPriorityQueue()
    for num in reversed(range(100)):
        lpq.push(num, num)

    heap_invariant(lpq.root)

    assert lpq.size == 100
    init_size = lpq.size
    for num in range(lpq.size):
        assert lpq.top() == num
        lpq.pop()
        init_size -= 1
        assert lpq.size == init_size


def test_k_th_smallest():
    lpq = LocalPriorityQueue()
    for num in reversed(range(1, 101)):
        lpq.push(num, num)

    heap_invariant(lpq.root)
    assert lpq.size == 100

    for num in reversed(range(1, 101)):
        assert lpq.k_th_smallest(num) == num
