package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;


public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private PriorityQueue<PQNode> pqTXState = new PriorityQueue<>();

    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    public void clearInternalState() {
        this.pqTXState.clear();
    }

    public Pair<Comparable, Object> currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        if (this.pqTXState.isEmpty()) {
            if (internalPQueue.root == null) {
                TXLibExceptions excep = new TXLibExceptions();
                throw excep.new PQueueIsEmptyException();
            }
            pqTXState.add(internalPQueue.root);
        }
        assert pqTXState.peek() != null;
        return new Pair<>(pqTXState.peek().priority, pqTXState.peek().value);
    }

    public void nextSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {

        if (this.dequeueCounter() >= internalPQueue.size() || internalPQueue.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }

        assert pqTXState.peek() != null;
        PQNode top = pqTXState.peek();

        try {
            pqTXState.remove();
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }

        if (top.left != null) {
            pqTXState.add(top.left);
        }
        if (top.right != null) {
            pqTXState.add(top.right);
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() == internalPQueue.size() && this.pqTXState.isEmpty()) || (this.dequeueCounter() < internalPQueue.size() && !this.pqTXState.isEmpty());
    }
}