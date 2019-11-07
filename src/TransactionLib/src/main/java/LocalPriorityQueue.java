package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Predicate;


public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private PriorityQueue<PQNode> pqTXState;
    private ArrayList<PQNode> _decreasingPriorityNodesState;

    public LocalPriorityQueue() {
        this.pqTXState = new PriorityQueue<PQNode>();
        this._decreasingPriorityNodesState = new ArrayList<PQNode>();
    }

    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    public void clearLocalState() {
        this._decreasingPriorityNodesState.clear();
        this.pqTXState.clear();
        this.pqTXState = null;
        this._decreasingPriorityNodesState = null;
    }

    public PQNode currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.pqTXState.isEmpty() || this.removeModifiedNode(this.pqTXState.peek())) {
            this.nextSmallest(internalPQueue);
        }

        assert pqTXState.peek() != null;
        return new PQNode(pqTXState.peek());
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        assert this.dequeueCounter() <= internalPQueue.size();
        if (this.dequeueCounter() == internalPQueue.size() || internalPQueue.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        if (this.pqTXState.isEmpty()) {
            pqTXState.add(internalPQueue.root);//O(log(dequeueCounter))
            return;
        }

        assert pqTXState.peek() != null;
        PQNode top = pqTXState.peek();//O(1)

        try {
            pqTXState.remove();//O(log(dequeueCounter))
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }

        if (top.getLeft() != null) {
            pqTXState.add(top.getLeft());
        }
        if (top.getRight() != null) {
            pqTXState.add(top.getRight());
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() == internalPQueue.size() && this.pqTXState.isEmpty()) || (this.dequeueCounter() < internalPQueue.size() && !this.pqTXState.isEmpty());
    }

    public void addModifiedNode(PQNode modifiedNode) {//public for test use only
        assert Collections.binarySearch(this._decreasingPriorityNodesState, modifiedNode) < 0;
        int index = -1 - Collections.binarySearch(this._decreasingPriorityNodesState, modifiedNode);
        this._decreasingPriorityNodesState.add(index, modifiedNode);
    }

    private boolean removeModifiedNode(PQNode modifiedNode) {
        if (this._decreasingPriorityNodesState.isEmpty()) {
            return false;
        }
        int index = Collections.binarySearch(this._decreasingPriorityNodesState, modifiedNode);
        if (-1 < index) {
            this._decreasingPriorityNodesState.remove(index);
            return true;
        }
        return false;
    }

    public ArrayList<PQNode> getDecreasingPriorityNodesState() {
        return this._decreasingPriorityNodesState;
    }

    public int getDecreasingPriorityNodesCounter() {
        return this._decreasingPriorityNodesState.size();
    }

    public void mergingPrimitivePriorityQueue(PrimitivePriorityQueue pQueue) {
        Predicate<PQNode> _isNotModifiedNode = pqNode -> Collections.binarySearch(this._decreasingPriorityNodesState, pqNode) < 0;
        this.mergingPrimitivePriorityQueue(pQueue, _isNotModifiedNode);
    }
}