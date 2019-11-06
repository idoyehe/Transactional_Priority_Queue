package TransactionLib.src.main.java;


import java.util.ArrayList;
import java.util.Collections;


public class PrimitivePriorityQueue {
    private static long PrimitivePriorityQueueRef = 0;
    protected long time = 0;
    protected ArrayList<PQObject> _heapContainer;
    protected int _ignoredCounter = 0;
    private long myRef;


    public PrimitivePriorityQueue() {
        this._heapContainer = new ArrayList<PQObject>();
        this.myRef = PrimitivePriorityQueue.PrimitivePriorityQueueRef++;
    }

    public PrimitivePriorityQueue(int minCapacity) {
        this._heapContainer = new ArrayList<PQObject>(minCapacity);
    }

    // to get index of parent of node at index i
    private int parent(int i) {
        return (i - 1) / 2;
    }

    // to get index of left child of node at index i
    private int left(int i) {
        return (2 * i + 1);
    }

    // to get index of right child of node at index i
    private int right(int i) {
        return (2 * i + 2);
    }

    private void swap(int i, int j) {
        PQObject iObject = this._heapContainer.get(i);
        PQObject jObject = this._heapContainer.get(j);
        this._heapContainer.set(i, jObject);
        this._heapContainer.set(j, iObject);

    }

    private void minHeapify(int i) {
        int heapSize = this._heapContainer.size() - 1;
        int l = left(i);
        int r = right(i);
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


    private PQObject singleDequeue() throws TXLibExceptions.PQueueIsEmptyException {
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
        if (this.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQObject root = this._heapContainer.get(0);
        assert !root.getIsIgnored();
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

        node2enqueue.setTime(this.time++);
        node2enqueue.setRef(this.myRef);
        int i = this._heapContainer.size();
        this._heapContainer.add(node2enqueue);
        // Fix the min heap property if it is violated
        while (i != 0 && this._heapContainer.get(parent(i)).compareTo(this._heapContainer.get(i)) > 0) {
            swap(i, parent(i));
            i = parent(i);
        }
        return node2enqueue;
    }


    public final PQObject enqueue(Comparable priority, Object value) {
        PQObject newNode = new PQObject(priority, value);
        return this.enqueue(newNode);
    }


//    public void modifyPriority(final PQObject nodeToModify, Comparable newPriority) {TODO:
//        assert this.containsNode(nodeToModify);
//
//        harr[i] = new_val;
//        while (i != 0 && harr[parent(i)] > harr[i]) {
//            swap( & harr[i], &harr[parent(i)]);
//            i = parent(i);
//        }
//    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public int size() {
        int size = this._heapContainer.size() - this._ignoredCounter;
        assert size >= 0;
        return size;
    }

    boolean containsNode(PQObject node) {
        return node.getRef() == this.myRef;
    }

    long getTime() {
        return this.time;
    }

    public void incrementIgnoredCounter() {//only for tests
        this._ignoredCounter++;
    }
}