package TransactionLib.src.main.java;

import java.lang.Exception;
import java.util.function.Predicate;

/**
 * This is the basic implementation of the Priority Queue it supports the conventional priority queue API
 * FOR COMPLEXITY CALCULATION THE PRIORITY QUEUE SIZE IS N
 */
public class PrimitivePriorityQueue {
    protected PQNode root = null;
    private int size = 0;

    /**
     * getter of the priority queue size
     *
     * @return the actual size of the priority queue
     * @Complexity O(1)
     */
    public int size() {
        return this.size;
    }

    /**
     * predicate the check whether the priority queue is empty
     *
     * @return true iff the priority queue is empty
     * @Complexity O(1)
     */
    public boolean isEmpty() {
        return (size <= 0);
    }

    /**
     * getter of the head of the priority queue
     *
     * @return new node contains the head
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(1)
     */
    public PQNode top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new PQNode(this.root);
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }

    /**
     * searching a node with the specific index
     *
     * @param targetIndex the index to be found
     * @return a reference to the found node with the given index
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log N)
     */
    private PQNode getRef2NodeByIndex(int targetIndex) {
        if (1 > targetIndex || targetIndex > this.size) {
            return null;
        }
        assert this.root != null;
        return PQNode.nodeSearch(this.root, targetIndex);
    }

    /**
     * enqueue new node to the priority queue
     *
     * @param newNode the new node to be enqueued
     * @return a reference of the new node
     * @Complexity O(log N)
     */
    final PQNode enqueueAsNode(PQNode newNode) {
        newNode.setRight(null);
        newNode.setLeft(null);
        newNode.setIndex(++this.size);
        if (this.root == null) {
            this.root = newNode;
            return newNode;
        }

        PQNode newNodeFather = this.getRef2NodeByIndex(this.size / 2);//O(log N)
        if (newNode.getIndex() % 2 == PQNodeTurn.LEFT.getValue()) {
            assert newNodeFather != null;
            newNodeFather.setLeft(newNode);
        } else {
            assert newNode.getIndex() % 2 == PQNodeTurn.RIGHT.getValue();
            assert newNodeFather != null;
            newNodeFather.setRight(newNode);
        }

        this.root = this.root.nodeSiftUp(newNode);//O(log N)
        return newNode;
    }

    /**
     * enqueue new node with given priority and value to the priority queue
     *
     * @param priority priority of the new node
     * @param value    value of the new node
     * @return a reference of the new node
     * @Complexity O(log N)
     */
    public final PQNode enqueue(Comparable priority, Object value) {
        PQNode newNode = new PQNode(priority, value);
        return this.enqueueAsNode(newNode);
    }

    /**
     * dequeue from the priority queue it's root, the minimum priority node
     *
     * @return a reference of the dequeued node
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log N)
     */
    PQNode dequeueAsNode() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root == null) {
            assert this.size == 0;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQNode nodeToDequeue = this.root;

        if (this.size == 1) {
            this.root = null;
            this.size = 0;
            assert nodeToDequeue.getRight() == null;
            assert nodeToDequeue.getLeft() == null;
            return nodeToDequeue;
        }

        PQNode maxIndexNodeFather = this.getRef2NodeByIndex(this.size / 2);
        PQNode lastHeapElement;

        if (this.size % 2 == PQNodeTurn.LEFT.getValue()) {
            assert maxIndexNodeFather.getLeft() != null;
            lastHeapElement = maxIndexNodeFather.getLeft();
            maxIndexNodeFather.setLeft(null);
        } else {
            assert this.size % 2 == PQNodeTurn.RIGHT.getValue();
            assert maxIndexNodeFather.getRight() != null;
            lastHeapElement = maxIndexNodeFather.getRight();
            maxIndexNodeFather.setRight(null);
        }
        assert lastHeapElement != null;
        lastHeapElement.setRight(this.root.getRight());
        lastHeapElement.setLeft(this.root.getLeft());
        lastHeapElement.setIndex(1);

        this.root.setLeft(null);
        this.root.setRight(null);

        this.size--;
        this.root = lastHeapElement;
        this.root = this.root.nodeSiftDown();
        return nodeToDequeue;
    }

    /**
     * dequeue from the priority queue it's root, the minimum priority node
     *
     * @return a copy of the dequeued node
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log N)
     */
    public PQNode dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        PQNode dequeuedNode = this.dequeueAsNode();
        return new PQNode(dequeuedNode);
    }

    /**
     * predicate the check whether a specific node in the priority queue
     *
     * @param node node to be search for
     * @return true iff the given node is in the priority queue
     * @Complexity O(log N)
     */
    boolean containsNode(PQNode node) {
        return this.getRef2NodeByIndex(node.getIndex()) == node;
    }

    /**
     * decreasing priority of a specific node
     *
     * @param nodeToModify the node to be modified
     * @param newPriority  the new priority
     * @Complexity O(log N)
     */
    public void decreasePriority(final PQNode nodeToModify, Comparable newPriority) {
        assert nodeToModify != null;
        if (!this.containsNode(nodeToModify) || nodeToModify.compareTo(newPriority) <= 0) {
            return;
        }
        nodeToModify.setPriority(newPriority);
        this.root = this.root.nodeSiftUp(nodeToModify);
    }

    /**
     * merging a PrimitivePriorityQueue to this by a given predicate
     *
     * @param pQueue           PrimitivePriorityQueue to be merged (size is M)
     * @param mergingPredicate only node that return true for this will be merged
     * @Complexity O(M * log N)
     */
    void mergingPrimitivePriorityQueue(PrimitivePriorityQueue pQueue, Predicate<PQNode> mergingPredicate) {
        this.mergingPriorityQueuesAux(pQueue.root, mergingPredicate);
        pQueue.root = null;
        pQueue.size = 0;
    }

    /**
     * a reclusive merging of PrimitivePriorityQueue to this by a given predicate
     *
     * @param currentRoot      the current root to be examine
     * @param mergingPredicate only node that return true for this will be merged
     * @Complexity O(M * log N)
     */

    private void mergingPriorityQueuesAux(PQNode currentRoot, Predicate<PQNode> mergingPredicate) {
        if (currentRoot == null) {
            return;
        }
        this.mergingPriorityQueuesAux(currentRoot.getLeft(), mergingPredicate);
        this.mergingPriorityQueuesAux(currentRoot.getRight(), mergingPredicate);
        if (mergingPredicate.test(currentRoot)) {
            this.enqueueAsNode(currentRoot);
        } else {
            currentRoot.setLeft(null);//detached un merged node from it's sons
            currentRoot.setRight(null);
        }
    }

    /**
     * FOR UNIT TEST USE ONLY
     * testing all the heap invariants
     *
     * @param root the root of the heap
     * @throws Exception
     */

    private static void testHeapInvariantRecursiveAUX(PQNode root) throws Exception {
        if (root == null) {
            return;
        }
        if (root.getRight() != null && root.compareTo(root.getRight()) > 0) {
            throw new Exception("Heap invariant is violated");
        }
        if (root.getRight() != null && root.getIndex() != root.getRight().getIndex() / 2) {
            throw new Exception("Heap invariant is violated");
        }
        if (root.getLeft() != null && root.compareTo(root.getLeft()) > 0) {
            throw new Exception("Heap invariant is violated");
        }
        if (root.getLeft() != null && root.getIndex() != root.getLeft().getIndex() / 2) {
            throw new Exception("Heap invariant is violated");
        }

        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(root.getLeft());
        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(root.getRight());
    }

    /**
     * FOR UNIT TEST USE ONLY
     * testing all the heap invariants
     *
     * @throws Exception
     */
    public void testHeapInvariantRecursive() throws Exception {
        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(this.root);
    }
}