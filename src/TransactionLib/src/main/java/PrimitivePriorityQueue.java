package TransactionLib.src.main.java;


import java.util.ArrayList;
import java.util.Collections;


public class PrimitivePriorityQueue {
    protected long time = 0;
    protected ArrayList<PQObject> _sortedArray;
    private PQObject _topRef = null;
    protected int _ignoredCounter = 0;


    public PrimitivePriorityQueue() {
        this._sortedArray = new ArrayList<PQObject>();
    }

    public PrimitivePriorityQueue(int minCapacity) {
        this._sortedArray = new ArrayList<PQObject>(minCapacity);
    }


    protected final PQObject enqueue(PQObject node2enqueue) {
        assert !node2enqueue.getIsIgnored();
        node2enqueue.setTime(this.time++);
        int index = -1 - Collections.binarySearch(this._sortedArray, node2enqueue);
        this._sortedArray.add(index, node2enqueue);
        this._topRef = null;
        return node2enqueue;
    }


    public final PQObject enqueue(Comparable priority, Object value) {
        PQObject newNode = new PQObject(priority, value);
        return this.enqueue(newNode);
    }

    public void modifyPriority(final PQObject nodeToModify, Comparable newPriority) {
        assert this.containsNode(nodeToModify);//checking node is actually part of the heap
        if (nodeToModify.getPriority() == newPriority) {
            return;
        }
        int index = Collections.binarySearch(this._sortedArray, nodeToModify);
        this._sortedArray.remove(index);
        nodeToModify.setPriority(newPriority);
        int newIndex = -1 - Collections.binarySearch(this._sortedArray, nodeToModify);
        this._sortedArray.add(newIndex, nodeToModify);
        this._topRef = null;
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public int size() {
        int size = this._sortedArray.size() - this._ignoredCounter;
        assert size >= 0;
        return size;
    }

    public PQObject dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        PQObject ret;
        try {
            ret = this._sortedArray.remove(0);
            while (ret.getIsIgnored()) {
                this._ignoredCounter--;
                ret = this._sortedArray.remove(0);
            }

        } catch (IndexOutOfBoundsException e) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        } finally {
            this._topRef = null;
        }
        return ret;
    }

    public PQObject top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        if (this._topRef == null) {
            int currentIndex = 0;
            do {

                this._topRef = this._sortedArray.get(currentIndex++);
            }
            while (this._topRef.getIsIgnored());

        }
        return this._topRef;
    }

    public PQObject topWithClearIgnored() throws TXLibExceptions.PQueueIsEmptyException {
        if (this._topRef == null) {
            try {
                this._topRef = this._sortedArray.get(0);
                while (this._topRef.getIsIgnored()) {
                    this._sortedArray.remove(0);
                    this._ignoredCounter--;
                    this._topRef = this._sortedArray.get(0);
                }
            } catch (IndexOutOfBoundsException e) {
                this._topRef = null;
                TXLibExceptions excep = new TXLibExceptions();
                throw excep.new PQueueIsEmptyException();
            }

        }
        return this._topRef;
    }

    boolean containsNode(PQObject node) {
        int index = Collections.binarySearch(this._sortedArray, node);
        return index > -1;
    }

    long getTime() {
        return this.time;
    }

    public void incrementIgnoredCounter() {//only for tests
        this._ignoredCounter++;
    }
}