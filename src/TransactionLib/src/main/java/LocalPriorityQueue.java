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
        PQNode top = lpq.peek();

        try {
            lpq.remove();
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }

        if (top.left != null) {
            lpq.add(top.left);
        }
        if (top.right != null) {
            lpq.add(top.right);
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() == internalPQueue.size && this.lpq.isEmpty()) || (this.dequeueCounter() < internalPQueue.size && !this.lpq.isEmpty());
    }
}