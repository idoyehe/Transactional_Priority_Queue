package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;


public class LocalPriorityQueue extends PrimitivePriorityQueue {
    private int _dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me
    private PriorityQueue<PQNode> pqTXState = new PriorityQueue<>();
    private ArrayList<PQNode> modifiedNodesState = new ArrayList<PQNode>();

    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    public void clearInternalState() {
        this.modifiedNodesState.clear();
        this.pqTXState.clear();
        this.pqTXState = null;
        this.modifiedNodesState = null;
    }

    public Pair<Comparable, Object> currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        if (this.pqTXState.isEmpty()) {
            if (internalPQueue.isEmpty()) {
                TXLibExceptions excep = new TXLibExceptions();
                throw excep.new PQueueIsEmptyException();
            }
            pqTXState.add(internalPQueue.root);
        }
        if (this.modifiedNodesState.contains(this.pqTXState.peek())) {
            this.modifiedNodesState.remove(this.pqTXState.peek());
            this.nextSmallest(internalPQueue);
        }

        if (this.dequeueCounter() >= internalPQueue.size()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
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
        if (this.pqTXState.peek() != null && this.modifiedNodesState.contains(this.pqTXState.peek())) {
            this.modifiedNodesState.remove(this.pqTXState.peek());
            this.nextSmallest(internalPQueue);
        }
    }

    public void addModifiedNode(PQNode modifiedNode) {
        assert !this.modifiedNodesState.contains(modifiedNode);
        this.modifiedNodesState.add(modifiedNode);
    }

    public ArrayList<PQNode> getModifiedNodesState() {
        return this.modifiedNodesState;
    }

    public int modifiedNodesCounter() {
        return this.modifiedNodesState.size();
    }
}