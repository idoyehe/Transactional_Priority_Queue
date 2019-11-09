package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * This class maneges the local state of a Priority Queue during transaction
 * FOR COMPLEXITY CALCULATION THIS SIZE IS K
 */
public class LocalPriorityQueue extends PrimitivePriorityQueue {
    /**
     * number of dequeue in the transactional priority queue during the transaction
     * FOR COMPLEXITY CALCULATION THIS SIZE IS D
     */
    private int _dequeueCounter = 0;
    /**
     * local state of dequeue simulation
     */
    private PriorityQueue<PQNode> pqTXState;
    /**
     * local state of all decreasing nodes
     * FOR COMPLEXITY CALCULATION THIS SIZE IS Q
     */
    private ArrayList<PQNode> _decreasingPriorityNodesState;
    /**
     * transactional priority queue (not local queue) locked by me
     */
    boolean isLockedByMe = false; // is queue (not local queue) locked by me

    /**
     * predicate return true iff node is in modified local state
     */
    Predicate<PQNode> _isModifiedNode = pqNode -> Collections.binarySearch(this._decreasingPriorityNodesState, pqNode) >= 0;
    /**
     * predicate return true iff node is NOT in modified local state
     */
    Predicate<PQNode> _isNotModifiedNode = pqNode -> Collections.binarySearch(this._decreasingPriorityNodesState, pqNode) < 0;

    /**
     * constructor
     */
    public LocalPriorityQueue() {
        this.pqTXState = new PriorityQueue<PQNode>();
        this._decreasingPriorityNodesState = new ArrayList<PQNode>();
    }

    /**
     * getter of simulated dequeue
     *
     * @return number of simulated dequeues
     * @Complexity O(1)
     */
    public int dequeueCounter() {
        return this._dequeueCounter;
    }

    /**
     * clearing all local state resources
     *
     * @Complexity O(Q + D)
     */
    public void clearLocalState() {
        this._decreasingPriorityNodesState.clear();
        this.pqTXState.clear();
        this.pqTXState = null;
        this._decreasingPriorityNodesState = null;
    }

    /**
     * getter of the current smallest node while dequeue simulation
     *
     * @param internalPQueue the queue to be simulated
     * @return a copy of the current smallest node in the simulation
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log Q * log D)
     */
    public PQNode currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.pqTXState.isEmpty() || this._isModifiedNode.test(this.pqTXState.peek())) {
            this.nextSmallest(internalPQueue);
        }

        assert pqTXState.peek() != null;
        return new PQNode(pqTXState.peek());
    }

    /**
     * getter of the next smallest node while dequeue simulation
     *
     * @param internalPQueue the queue to be simulated
     * @return a reference of the current smallest node in the simulation
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log D)
     */
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
            pqTXState.remove();//O(log(D))
        } catch (NoSuchElementException e) {
            assert false; //shouldn't be here
        }

        if (top.getLeft() != null) {
            pqTXState.add(top.getLeft());//O(log(D))
        }
        if (top.getRight() != null) {
            pqTXState.add(top.getRight());//O(log(D))
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() == internalPQueue.size() && this.pqTXState.isEmpty()) || (this.dequeueCounter() < internalPQueue.size() && !this.pqTXState.isEmpty());
    }

    /**
     * adding a new modified node the local state
     *
     * @param modifiedNode node to be added
     * @Complexity O(Q)
     */
    public void addModifiedNode(PQNode modifiedNode) {//public for test use only
        int index = -1 - Collections.binarySearch(this._decreasingPriorityNodesState, modifiedNode);
        assert index > -1;
        this._decreasingPriorityNodesState.add(index, modifiedNode);
    }

    /**
     * getter of the modified node state
     *
     * @return the state of the modified node during transaction
     */
    public ArrayList<PQNode> getDecreasingPriorityNodesState() {
        return this._decreasingPriorityNodesState;
    }

    /**
     * getter of the modified node counter
     *
     * @return the amount of the modified node during transaction
     */
    public int getDecreasingPriorityNodesCounter() {
        return this._decreasingPriorityNodesState.size();
    }

    /**
     * merging the transactional priority queue into the local state
     *
     * @param pQueue the transactional priority queue to be merged
     * @Complexity O(N * logQ + N * logK)
     */
    public void mergingPrimitivePriorityQueue(PrimitivePriorityQueue pQueue) {
        this.mergingPrimitivePriorityQueue(pQueue, _isNotModifiedNode);
    }
}