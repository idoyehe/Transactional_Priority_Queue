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

    public void mergingPriorityQueuesWithoutModification(PrimitivePriorityQueue pQueue) {
        ArrayList<PQNode> sorted1 = this.sortedArray;
        ArrayList<PQNode> sorted2 = pQueue.sortedArray;

        sorted2.removeIf(this::removeModifiedNode);//removing all modified nodes from the array

        assert this.modifiedNodesCounter() == 0;

        int totalSize = sorted1.size() + sorted2.size();
        this.sortedArray = new ArrayList<PQNode>(totalSize);//allocating new Array
        this.time = 0;
        int it1 = 0, it2 = 0;
        // Traverse both array
        while (it1 < sorted1.size() && it2 < sorted2.size()) {
            // Check if current element of first
            // array is smaller than current element
            // of second array. If yes, store first
            // array element and increment first array
            // index. Otherwise do same with second array
            PQNode node1 = sorted1.get(it1);
            PQNode node2 = sorted2.get(it2);
            if (node1.compareTo(node2) < 0) {
                node1.setTime(this.time);
                this.sortedArray.add(node1);
                it1++;
            } else {
                node2.setTime(this.time);
                this.sortedArray.add(node2);
                it2++;
            }
            this.time++;
        }

        // Store remaining elements of first array
        while (it1 < sorted1.size()) {
            this.sortedArray.add(sorted1.get(it1++).setTime(this.time++));
        }

        // Store remaining elements of second array
        while (it2 < sorted2.size())
            this.sortedArray.add(sorted2.get(it2++).setTime(this.time++));

        assert it1 == sorted1.size();
        assert it2 == sorted2.size();
        assert this.sortedArray.size() == totalSize;

        sorted1.clear();
        sorted2.clear();
    }
}