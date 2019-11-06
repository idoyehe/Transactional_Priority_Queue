package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private ArrayList<PQObject> _ignoredElemntsState;
    Iterator<PQObject> iterator = null;
    PQObject currentSmallest = null;

    public LocalPriorityQueue() {
        this(0);
    }

    public LocalPriorityQueue(long startTime) {
        this.time = startTime;
        this._ignoredElemntsState = new ArrayList<PQObject>();
    }

    public int dequeueCounter() {
        return this._dequeueCounter;
    }


    public void clearInternalState() {
        this._ignoredElemntsState = null;
        this.iterator = null;
        this.currentSmallest = null;
    }

    public PQObject currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.currentSmallest == null || this.currentSmallest.getIsIgnored() || this.removeModifiedElementFromState(this.currentSmallest)) {
            this.nextSmallest(internalPQueue);
        }

        return new PQObject(currentSmallest);
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {


        if (internalPQueue.isEmpty() || this.dequeueCounter() == internalPQueue._heapContainer.size()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        if (this.iterator == null) {
            this.iterator = internalPQueue._heapContainer.iterator();
        } else {
            this._dequeueCounter++;
        }
        if (iterator.hasNext()) {
            this.currentSmallest = iterator.next();
        } else {
            this.iterator = null;
            this.currentSmallest = null;
        }

        assert (this.dequeueCounter() == internalPQueue._heapContainer.size() && this.iterator == null ||
                (this.dequeueCounter() < internalPQueue._heapContainer.size()));
    }

    public void addModifiedElementFromState(PQObject modifiedObject) {
        assert !this._ignoredElemntsState.contains(modifiedObject);
        int index = -1 - Collections.binarySearch(this._ignoredElemntsState, modifiedObject);
        this._ignoredElemntsState.add(index, modifiedObject);
    }

    boolean removeModifiedElementFromState(PQObject modifiedObject) {
        if (this.getIgnoredElemntsState().isEmpty()) {
            return false;
        }
        int index = Collections.binarySearch(this._ignoredElemntsState, modifiedObject);
        if (-1 < index) {
            this._ignoredElemntsState.remove(index);
            return true;
        }
        return false;
    }

    public final ArrayList<PQObject> getIgnoredElemntsState() {
        return this._ignoredElemntsState;
    }


    public void mergingPriorityQueues(PrimitivePriorityQueue pQueue) {
        for (PQObject element : this._heapContainer) {
            pQueue.enqueue(element);
        }

        for (PQObject element : this.getIgnoredElemntsState()) {
            element.setIgnored();
        }
        pQueue._ignoredCounter += this.getIgnoredElemntsState().size();
        this._heapContainer.clear();
        this._ignoredElemntsState.clear();
    }
}