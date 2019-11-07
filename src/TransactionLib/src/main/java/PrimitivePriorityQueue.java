package TransactionLib.src.main.java;

import java.util.ArrayList;


public class PrimitivePriorityQueue {
    protected ArrayList<PQObject> _heapContainer;
    protected int _ignoredCounter = 0;

    public PrimitivePriorityQueue() {
        this._heapContainer = new ArrayList<PQObject>();
    }

    public PrimitivePriorityQueue(int minCapacity) {
        this._heapContainer = new ArrayList<PQObject>(minCapacity);
    }

    private void swap(int i, int j) {
        PQObject iObject = this._heapContainer.get(i);
        PQObject jObject = this._heapContainer.get(j);

        jObject.setIndex(i);//indexes swapping
        iObject.setIndex(j);

        this._heapContainer.set(i, jObject);
        this._heapContainer.set(j, iObject);

    }

    private void minHeapify(int i) {
        int heapSize = this._heapContainer.size();
        int l = PQObject.left(i);
        int r = PQObject.right(i);
        int smallest = i;

        if (l < heapSize && this._heapContainer.get(l).compareTo(this._heapContainer.get(i)) < 0)
            smallest = l;

        if (r < heapSize && this._heapContainer.get(r).compareTo(this._heapContainer.get(smallest)) < 0)
            smallest = r;

        if (smallest != i) {
            swap(i, smallest);
            this.minHeapify(smallest);
        }
    }


    PQObject singleDequeue() throws TXLibExceptions.PQueueIsEmptyException {
        int heapSize = this._heapContainer.size();
        if (heapSize <= 0) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQObject root = this._heapContainer.get(0);
        this._ignoredCounter -= root.getIsIgnored() ? 1 : 0;
        if (heapSize == 1) {
            return this._heapContainer.remove(0);//O(1) because this is last element in the array
        }

        // Store the minimum value, and remove it from heap
        this._heapContainer.set(0, this._heapContainer.remove(heapSize - 1));//O(1) because this is last element in the array
        this.minHeapify(0);
        return root;
    }

    public PQObject dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        PQObject root;
        do {
            root = this.singleDequeue();
        }
        while (root.getIsIgnored());
        return root;
    }

    public PQObject top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this._heapContainer.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQObject root = this._heapContainer.get(0);
        return root;
    }


    public PQObject topWithClearIgnored() throws TXLibExceptions.PQueueIsEmptyException {
        try {
            while (this._heapContainer.get(0).getIsIgnored()) {
                this.singleDequeue();
            }
        } catch (IndexOutOfBoundsException e) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        return this._heapContainer.get(0);
    }


    protected final PQObject enqueue(PQObject node2enqueue) {
        assert !node2enqueue.getIsIgnored();
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


    public final PQObject enqueue(Comparable priority, Object value) {
        PQObject newNode = new PQObject(priority, value);
        return this.enqueue(newNode);
    }


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

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public int size() {
        int size = this._heapContainer.size() - this._ignoredCounter;
        assert size >= 0;
        return size;
    }

    public boolean containsNode(PQObject node) {
        int nodeIndex = node.getIndex();
        if (nodeIndex < 0 || nodeIndex > this._heapContainer.size()) {
            return false;
        }
        return this._heapContainer.get(nodeIndex) == node;
    }


    public void incrementIgnoredCounter() {//only for tests
        this._ignoredCounter++;
    }
}