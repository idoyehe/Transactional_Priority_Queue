package TransactionLib.src.main.java;


import java.util.ArrayList;
import java.util.Collections;


public class PrimitivePriorityQueue {
    protected long time = 0;
    ArrayList<PQNode> sortedArray;


    public PrimitivePriorityQueue() {
        this.sortedArray = new ArrayList<PQNode>();
    }

    public PrimitivePriorityQueue(int minCapacity) {
        this.sortedArray = new ArrayList<PQNode>(minCapacity);
    }


    public final PQNode enqueue(Comparable priority, Object value) {
        PQNode newNode = new PQNode(priority, value,this.time++);
        int index = -1 - Collections.binarySearch(this.sortedArray, newNode);
        this.sortedArray.add(index, newNode);
        return newNode;
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
}