package TransactionLib.src.main.java;

import java.util.*;
import java.util.PriorityQueue;
import java.util.function.Predicate;

public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private ArrayList<PQObject> _ignoredElementsState = new ArrayList<PQObject>();
    private PriorityQueue<PQObject> pqTXState = new PriorityQueue<>();
    Iterator<PQObject> iterator = null;

    public int dequeueCounter() {
        return this._dequeueCounter;
    }


    public void clearInternalState() {
        assert _ignoredElementsState.size() == 0;
        this.pqTXState.clear();
        this._ignoredElementsState = null;
        this.iterator = null;
        this.pqTXState = null;
    }

    public PQObject currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.pqTXState.peek() == null || this.removeModifiedElementFromState(this.pqTXState.peek())) {
            this.nextSmallest(internalPQueue);
        }

        return new PQObject(this.pqTXState.peek());
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        if (internalPQueue._heapContainer.isEmpty() || this.dequeueCounter() == internalPQueue._heapContainer.size()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        if (this.pqTXState.isEmpty()) {
            pqTXState.add(internalPQueue._heapContainer.get(0));//O(log(dequeueCounter))
            return;
        }

        assert pqTXState.peek() != null;
        PQObject top = pqTXState.peek();//O(1)

        try {
            pqTXState.remove();//O(log(dequeueCounter))
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }

        int leftIndex = PQObject.left(top.getIndex());
        int rightIndex = PQObject.right(top.getIndex());

        if (leftIndex < internalPQueue._heapContainer.size()) {
            pqTXState.add(internalPQueue._heapContainer.get(leftIndex));
        }
        if (rightIndex < internalPQueue._heapContainer.size()) {
            pqTXState.add(internalPQueue._heapContainer.get(rightIndex));
        }

        this._dequeueCounter++;
//        assert (this.dequeueCounter() < internalPQueue._heapContainer.size() && !this.pqTXState.isEmpty()) ||
//                (this.dequeueCounter() == internalPQueue._heapContainer.size() && this.pqTXState.isEmpty());
        if (this.dequeueCounter() == internalPQueue._heapContainer.size()) {
            assert this.pqTXState.isEmpty();
        } else {
            assert this.dequeueCounter() < internalPQueue._heapContainer.size();
            assert !this.pqTXState.isEmpty();
        }
    }


    public void addModifiedElementFromState(PQObject modifiedObject) {
        assert !this._ignoredElementsState.contains(modifiedObject);
        int index = -1 - Collections.binarySearch(this._ignoredElementsState, modifiedObject);
        this._ignoredElementsState.add(index, modifiedObject);
    }

    boolean removeModifiedElementFromState(PQObject modifiedObject) {
        if (this.getIgnoredElemntsState().isEmpty()) {
            return false;
        }
        int index = Collections.binarySearch(this._ignoredElementsState, modifiedObject);
        if (-1 < index) {
            this._ignoredElementsState.remove(index);
            return true;
        }
        return false;
    }

    public final ArrayList<PQObject> getIgnoredElemntsState() {
        return this._ignoredElementsState;
    }


    public void mergingPriorityQueues(PrimitivePriorityQueue pQueue) {
        Predicate<PQObject> _isNotModifiedNode = pqNode -> Collections.binarySearch(this._ignoredElementsState, pqNode) >= 0;

        pQueue._heapContainer.removeIf(_isNotModifiedNode);

        for (PQObject element : pQueue._heapContainer) {
            this.enqueue(element);
        }

        pQueue._heapContainer.clear();
        this._ignoredElementsState.clear();
    }
}