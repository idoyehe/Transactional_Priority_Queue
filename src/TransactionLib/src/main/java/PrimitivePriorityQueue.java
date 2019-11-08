package TransactionLib.src.main.java;

import java.util.ArrayList;

/**
 * This is the basic implementation of the Priority Queue it supports the conventional priority queue API
 * FOR COMPLEXITY CALCULATION THE PRIORITY QUEUE SIZE IS N
 */
public class PrimitivePriorityQueue {
    protected ArrayList<PQObject> _heapContainer;

    /**
     * simple constructor
     */
    public PrimitivePriorityQueue() {
        this._heapContainer = new ArrayList<PQObject>();
    }

    /**
     * initialize size constructor
     */
    public PrimitivePriorityQueue(int minCapacity) {
        this._heapContainer = new ArrayList<PQObject>(minCapacity);
    }

    /**
     * swapping between 2 location in the container
     *
     * @param i first index
     * @param j second index
     * @Complexity O(1)
     */
    private void swap(int i, int j) {
        PQObject iObject = this._heapContainer.get(i);
        PQObject jObject = this._heapContainer.get(j);

        jObject.setIndex(i);//indexes swapping
        iObject.setIndex(j);

        this._heapContainer.set(i, jObject);
        this._heapContainer.set(j, iObject);

    }

    /**
     * fixing the heap stating given index
     *
     * @param index index to start with fixing
     * @Complexity O(log N)
     */
    private void minHeapify(int index) {
        int heapSize = this._heapContainer.size();
        int l = PQObject.left(index);
        int r = PQObject.right(index);
        int smallest = index;

        if (l < heapSize && this._heapContainer.get(l).compareTo(this._heapContainer.get(index)) < 0)
            smallest = l;

        if (r < heapSize && this._heapContainer.get(r).compareTo(this._heapContainer.get(smallest)) < 0)
            smallest = r;

        if (smallest != index) {
            swap(index, smallest);
            this.minHeapify(smallest);
        }
    }

    /**
     * dequeue from the priority queue it's root, the minimum priority node
     *
     * @return a reference of the dequeued node
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log N)
     */
    public PQObject dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        int heapSize = this._heapContainer.size();
        if (heapSize <= 0) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQObject root = this._heapContainer.get(0);
        if (heapSize == 1) {
            return this._heapContainer.remove(0);//O(1) because this is last element in the array
        }

        // Store the minimum value, and remove it from heap
        this._heapContainer.set(0, this._heapContainer.remove(heapSize - 1));//O(1) because this is last element in the array
        this.minHeapify(0);
        return root;
    }

    /**
     * getter of the head of the priority queue
     *
     * @return new node contains the head
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(1)
     */
    public PQObject top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        return this._heapContainer.get(0);
    }

    /**
     * enqueue new node to the priority queue
     *
     * @param node2enqueue the new node to be enqueued
     * @return a reference of the new node
     * @Complexity amortized O(log N)
     */
    protected final PQObject enqueue(PQObject node2enqueue) {
        // First insert the new key at the end

        int i = this._heapContainer.size();
        node2enqueue.setIndex(i);
        this._heapContainer.add(node2enqueue);
        // Fix the min heap property if it is violated
        while (i != 0 && this._heapContainer.get(PQObject.parent(i)).compareTo(this._heapContainer.get(i)) > 0) {
            swap(i, PQObject.parent(i));
            i = PQObject.parent(i);
        }
        return node2enqueue;
    }

    /**
     * enqueue new node with given priority and value to the priority queue
     *
     * @param priority priority of the new node
     * @param value    value of the new node
     * @return a reference of the new node
     * @Complexity amortized O(log N)
     */
    public final PQObject enqueue(Comparable priority, Object value) {
        PQObject newNode = new PQObject(priority, value);
        return this.enqueue(newNode);
    }

    /**
     * dequeue from the priority queue it's root, the minimum priority node
     *
     * @return a reference of the dequeued node
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log N)
     */
    public void decreasePriority(final PQObject nodeToModify, Comparable newPriority) {
        assert nodeToModify != null;
        if (!this.containsNode(nodeToModify) || nodeToModify.compareTo(newPriority) <= 0) {
            return;
        }
        nodeToModify.setPriority(newPriority);
        int i = nodeToModify.getIndex();
        while (i != 0 && this._heapContainer.get(PQObject.parent(i)).compareTo(this._heapContainer.get(i)) > 0) {
            swap(i, PQObject.parent(i));
            i = PQObject.parent(i);
        }
    }

    /**
     * predicate the check whether the priority queue is empty
     *
     * @return true iff the priority queue is empty
     * @Complexity O(1)
     */
    public boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * getter of the priority queue size
     *
     * @return the actual size of the priority queue
     * @Complexity O(1)
     */
    public int size() {
        return this._heapContainer.size();
    }

    /**
     * predicate the check whether a specific node in the priority queue
     *
     * @param node node to be search for
     * @return true iff the given node is in the priority queue
     * @Complexity O(log N)
     */
    public boolean containsNode(PQObject node) {
        int nodeIndex = node.getIndex();
        if (nodeIndex < 0 || nodeIndex >= this._heapContainer.size()) {
            return false;
        }
        return this._heapContainer.get(nodeIndex) == node;
    }
}