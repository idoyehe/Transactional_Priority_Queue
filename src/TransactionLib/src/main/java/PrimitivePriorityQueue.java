package TransactionLib.src.main.java;

import java.lang.Exception;
import java.util.function.Predicate;

public class PrimitivePriorityQueue {
    protected PQNode root = null;
    private int size = 0;

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return (size <= 0);
    }

    public PQNode top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new PQNode(this.root);
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }


    private PQNode getRef2NodeByIndex(int targetIndex) {
        if (1 > targetIndex || targetIndex > this.size) {
            return null;
        }
        assert this.root != null;
        return PQNode.nodeSearch(this.root, targetIndex);
    }

    final PQNode enqueueAsNode(PQNode newNode) {
        newNode.setRight(null);
        newNode.setLeft(null);
        newNode.setIndex(++this.size);
        if (this.root == null) {
            this.root = newNode;
            return newNode;
        }

        PQNode newNodeFather = this.getRef2NodeByIndex(this.size / 2);
        if (newNode.getIndex() % 2 == PQNodeTurn.LEFT.getValue()) {
            newNodeFather.setLeft(newNode);
        } else {
            assert newNode.getIndex() % 2 == PQNodeTurn.RIGHT.getValue();
            newNodeFather.setRight(newNode);
        }

        this.root = this.root.nodeSiftUp(newNode);
        return newNode;
    }

    public final PQNode enqueue(Comparable priority, Object value) {
        PQNode newNode = new PQNode(priority, value);
        return this.enqueueAsNode(newNode);
    }

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

        this.size -= 1;
        this.root = lastHeapElement;
        this.root = this.root.nodeSiftDown();
        return nodeToDequeue;
    }

    public PQNode dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        PQNode dequeuedNode = this.dequeueAsNode();
        return new PQNode(dequeuedNode);
    }

    boolean containsNode(PQNode node) {
        return this.getRef2NodeByIndex(node.getIndex()) == node;
    }

    public void decreasePriority(final PQNode nodeToModify, Comparable newPriority) {
        assert nodeToModify != null;
        if (!this.containsNode(nodeToModify) || nodeToModify.compareTo(newPriority) <= 0) {
            return;
        }
        nodeToModify.setPriority(newPriority);
        this.root = this.root.nodeSiftUp(nodeToModify);
    }


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

    public void testHeapInvariantRecursive() throws Exception {
        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(this.root);
    }

    protected void mergingPrimitivePriorityQueue(PrimitivePriorityQueue pQueue, Predicate<PQNode> mergingPredicate) {
        this.mergingPriorityQueuesAux(pQueue.root, mergingPredicate);
        pQueue.root = null;
        pQueue.size = 0;
    }


    private void mergingPriorityQueuesAux(PQNode currentRoot, Predicate<PQNode> mergingPredicate) {
        if (currentRoot == null) {
            return;
        }
        this.mergingPriorityQueuesAux(currentRoot.getLeft(), mergingPredicate);
        this.mergingPriorityQueuesAux(currentRoot.getRight(), mergingPredicate);
        if (mergingPredicate.test(currentRoot)) {
            this.enqueueAsNode(currentRoot);
        }
    }
}