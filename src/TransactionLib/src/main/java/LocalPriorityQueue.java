package TransactionLib.src.main.java;

import java.util.*;
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
     * local state of all decreasing nodes
     * FOR COMPLEXITY CALCULATION THIS SIZE IS Q
     */
    private ArrayList<PQObject> _ignoredElementsState = new ArrayList<PQObject>();
    /**
     * local state of dequeue simulation
     */
    private PriorityQueue<PQObject> pqTXState = new PriorityQueue<>();
    /**
     * transactional priority queue (not local queue) locked by me
     */
    boolean isLockedByMe = false;
    /**
     * counter of new modified node to be removed in commit
     */
    private int _newRemovedModifiedNodesCounter = 0;

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
     * @Complexity O(D + Q)
     */
    public void clearInternalState() {
        this._ignoredElementsState.clear();
        this.pqTXState.clear();
        this._ignoredElementsState = null;
        this.pqTXState = null;
    }

    /**
     * getter of the current smallest node while dequeue simulation
     *
     * @param internalPQueue the queue to be simulated
     * @return a copy of the current smallest node in the simulation
     * @throws TXLibExceptions.PQueueIsEmptyException
     * @Complexity O(log Q * log D)
     */
    public PQObject currentSmallest(PrimitivePriorityQueue internalPQueue) throws TXLibExceptions.PQueueIsEmptyException {
        while (this.pqTXState.peek() == null || this.pqTXState.peek().getIsIgnored() || isModifiedNode(this.pqTXState.peek())) {
            this.nextSmallest(internalPQueue);
        }

        return new PQObject(this.pqTXState.peek());
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

        if (leftIndex < internalPQueue.containerSize()) {
            pqTXState.add(internalPQueue._heapContainer.get(leftIndex));
        }
        if (rightIndex < internalPQueue.containerSize()) {
            pqTXState.add(internalPQueue._heapContainer.get(rightIndex));
        }

        this._dequeueCounter++;
        assert (this.dequeueCounter() < internalPQueue.containerSize() && !this.pqTXState.isEmpty()) || (this.dequeueCounter() == internalPQueue.containerSize() && this.pqTXState.isEmpty());
    }

    /**
     * adding a new modified node the local state
     *
     * @param modifiedObject node to be added
     * @Complexity O(Q)
     */
    public void addModifiedElementFromState(PQObject modifiedObject) {
        assert !this._ignoredElementsState.contains(modifiedObject);
        int index = -1 - Collections.binarySearch(this._ignoredElementsState, modifiedObject);
        this._ignoredElementsState.add(index, modifiedObject);
    }

    /**
     * predicate return true iff node is in modified local state
     */
    private boolean isModifiedNode(PQObject pqObject) {
        boolean isAlreadyModified = Collections.binarySearch(this._ignoredElementsState, pqObject) >= 0;
        this._newRemovedModifiedNodesCounter += (isAlreadyModified) ? 1 : 0;
        return isAlreadyModified;
    }

    /**
     * getter of the modified node state
     *
     * @return the state of the modified node during transaction
     */
    public final ArrayList<PQObject> getIgnoredElementsState() {
        return this._ignoredElementsState;
    }

    /**
     * merging the transactional priority queue into the local state
     *
     * @param pQueue the transactional priority queue to be merged
     * @Complexity amortized O(K*logN + Q)
     */
    public void mergingPriorityQueues(PrimitivePriorityQueue pQueue) {
        int oldSize = pQueue.size();
        for (PQObject element : this._heapContainer) {
            pQueue.enqueue(element);
        }

        for (PQObject element : this.getIgnoredElementsState()) {
            element.setIgnored();
        }
        int newIgnoredAdd = this.getIgnoredElementsState().size() - this._newRemovedModifiedNodesCounter;
        pQueue._ignoredCounter += newIgnoredAdd;
        assert pQueue.size() == (oldSize + this._heapContainer.size() - newIgnoredAdd);
        this._heapContainer.clear();
        this._ignoredElementsState.clear();
    }
}