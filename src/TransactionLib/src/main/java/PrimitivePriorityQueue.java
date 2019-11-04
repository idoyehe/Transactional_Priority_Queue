package TransactionLib.src.main.java;


import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Predicate;


public class PrimitivePriorityQueue {
    ArrayList<PQNode> sortedArray;


    public PrimitivePriorityQueue() {
        this.sortedArray = new ArrayList<PQNode>();
    }

    public PrimitivePriorityQueue(int minCapacity) {
        this.sortedArray = new ArrayList<PQNode>(minCapacity);
    }


    final PQNode enqueueAsNode(PQNode newNode) {
        int index = -1 - Collections.binarySearch(this.sortedArray, newNode);
        this.sortedArray.add(index, newNode);
        return newNode; // here implementing binary
    }


    public final PQNode enqueue(Comparable priority, Object value) {
        PQNode newNode = new PQNode(priority, value);
        return this.enqueueAsNode(newNode);
    }

    public void decreasePriority(final PQNode nodeToModify, Comparable newPriority) {
        assert this.containsNode(nodeToModify);//checking node is actually part of the heap
        if (nodeToModify.getPriority().compareTo(newPriority) > 0) {
            int index = Collections.binarySearch(this.sortedArray, nodeToModify);
            this.sortedArray.remove(index);
            nodeToModify.setPriority(newPriority);
            int newIndex = -1 - Collections.binarySearch(this.sortedArray, nodeToModify);
            this.sortedArray.add(newIndex, nodeToModify);
        }
    }

    public boolean isEmpty() {
        return this.sortedArray.isEmpty();
    }

    public int size() {
        return this.sortedArray.size();
    }

    public PQNode dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        return this.sortedArray.remove(0);
    }

    public PQNode top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.isEmpty()) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        return this.sortedArray.get(0);
    }

    boolean containsNode(PQNode node) {
        int index = Collections.binarySearch(this.sortedArray, node);
        return index > -1;
    }


    public void mergingPriorityQueues(PrimitivePriorityQueue pQueue, Predicate<PQNode> removePredicate) {
        pQueue.sortedArray.removeIf(removePredicate);

        ArrayList<PQNode> oldSorted = this.sortedArray;

        int totalSize = oldSorted.size() + pQueue.sortedArray.size();
        this.sortedArray = new ArrayList<PQNode>(totalSize);
        int i = 0, j = 0, k = 0;
        // Traverse both array
        while (i < oldSorted.size() && j < pQueue.sortedArray.size()) {
            // Check if current element of first
            // array is smaller than current element
            // of second array. If yes, store first
            // array element and increment first array
            // index. Otherwise do same with second array
            PQNode node1 = oldSorted.get(i);
            PQNode node2 = pQueue.sortedArray.get(j);

            if (node1.compareTo(node2) < 0) {
                this.sortedArray.add(k++, node1);
                i++;
            } else {
                this.sortedArray.add(k++, node2);
                j++;
            }
        }

        // Store remaining elements of first array
        while (i < oldSorted.size())
            this.sortedArray.add(k++, oldSorted.get(i++));

        // Store remaining elements of second array
        while (j < pQueue.sortedArray.size())
            this.sortedArray.add(k++, pQueue.sortedArray.get(j++));

        assert k == totalSize && i == oldSorted.size() && j == pQueue.sortedArray.size();
        oldSorted.clear();
        pQueue.sortedArray.clear();
    }
}