package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;


public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    PriorityQueue<PQNode> lpq = new PriorityQueue<>(new PQNodeComparator());

    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    public Pair<Comparable, Object> currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        if (this.lpq.isEmpty()) {
            if (internalPQueue.root == null) {
                TXLibExceptions excep = new TXLibExceptions();
                throw excep.new PQueueIsEmptyException();
            }
            lpq.add(internalPQueue.root);
        }
        assert lpq.peek() != null;
        return new Pair<>(lpq.peek().priority, lpq.peek().value);
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {

        if (this.dequeueCounter() >= internalPQueue.size || internalPQueue.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        assert lpq.peek() != null;
        int topIndex = lpq.peek().index;

        try {
            lpq.remove();
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }
        int leftSonIndex = internalPQueue.leftSon(topIndex);
        int rightSonIndex = internalPQueue.rightSon(topIndex);

        if (leftSonIndex <= internalPQueue.size) {
            PQNode leftSon = internalPQueue.searchNode(leftSonIndex);
            lpq.add(leftSon);
        }

        if (rightSonIndex <= internalPQueue.size) {
            PQNode rightSon = internalPQueue.searchNode(rightSonIndex);
            lpq.add(rightSon);
        }
        this._dequeueCounter++;
    }
}