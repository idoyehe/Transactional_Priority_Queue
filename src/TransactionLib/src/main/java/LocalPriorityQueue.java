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
        return new Pair<>(pqTXState.peek().getPriority(), pqTXState.peek().getValue());
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

        if (top.getLeft() != null) {
            pqTXState.add(top.getLeft());
        }
        if (top.getRight() != null) {
            pqTXState.add(top.getRight());
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() == internalPQueue.size() && this.pqTXState.isEmpty()) || (this.dequeueCounter() < internalPQueue.size() && !this.pqTXState.isEmpty());
    }

//    public void modifyPriority(final PQNode nodeToModify, Comparable newPriority) {
//        assert this.findPQNode(nodeToModify.index) == nodeToModify;
//        if (nodeToModify.priority.compareTo(newPriority) > 0) {
//            nodeToModify.priority = newPriority;
//            PrimitivePriorityQueue.nodeSiftUp(nodeToModify);
//        } else {
//            nodeToModify.priority = newPriority;
//        }
//    }
}