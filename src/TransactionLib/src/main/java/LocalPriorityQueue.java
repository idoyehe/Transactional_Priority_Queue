package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private ArrayList<PQNode> modifiedNodesState = new ArrayList<PQNode>();
    Iterator<PQNode> iterator = null;
    PQNode currentSmallest = null;

    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    public void clearInternalState() {
        this.modifiedNodesState.clear();
        this.modifiedNodesState = null;
        this.iterator = null;
        this.currentSmallest = null;
    }

    public PQNode currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.currentSmallest == null || this.removeModifiedNode(this.currentSmallest)) {
            this.nextSmallest(internalPQueue);
        }

        return new PQNode(currentSmallest);
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {


        if (internalPQueue.isEmpty() || this.dequeueCounter() == internalPQueue.size()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        if (this.iterator == null) {
            this.iterator = internalPQueue.sortedArray.iterator();
        } else {
            this._dequeueCounter++;
        }
        if (iterator.hasNext()) {
            this.currentSmallest = iterator.next();
        } else {
            this.iterator = null;
            this.currentSmallest = null;
        }

        assert (this.dequeueCounter() == internalPQueue.size() && this.iterator == null ||
                (this.dequeueCounter() < internalPQueue.size()));
    }

    public void addModifiedNode(PQNode modifiedNode) {
        assert !this.modifiedNodesState.contains(modifiedNode);
        int index = -1 - Collections.binarySearch(this.modifiedNodesState, modifiedNode);
        this.modifiedNodesState.add(index, modifiedNode);
    }

    boolean removeModifiedNode(PQNode modifiedNode) {
        int index = Collections.binarySearch(this.modifiedNodesState, modifiedNode);
        if (-1 < index) {
            this.modifiedNodesState.remove(index);
            return true;
        }
        return false;
    }

    public ArrayList<PQNode> getModifiedNodesState() {
        return this.modifiedNodesState;
    }

    public int modifiedNodesCounter() {
        return this.modifiedNodesState.size();
    }
}