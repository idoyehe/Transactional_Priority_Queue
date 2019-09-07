package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.ArrayList;

public class LocalPriorityQueue {
    protected PQNode root = null;
    public int size = 0;
    protected int dequeueCounter = 0; // how many dequeue in transaction
    protected boolean isLockedByMe = false; // is queue (not local queue) locked by me

    protected boolean isEmpty() {
        return (size <= 0);
    }


    public Object top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return this.root.value;
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }

    private PQNode _search_node_(int index) throws TXLibExceptions.PQIndexNotFound {
        if (index <= 0 || index > this.size) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQIndexNotFound();
        }

        ArrayList<Integer> binaryDigits = new ArrayList<>();
        String binaryIndex = Integer.toBinaryString(index);
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits.add(Character.getNumericValue(binaryIndex.charAt(i)));
        }
        return this.root.search(index, binaryDigits);
    }

    protected void enqueue(Comparable priority, Object val) throws TXLibExceptions.PQIndexNotFound {

        PQNode newNode = new PQNode();
        newNode.right = null;
        newNode.left = null;
        newNode.priority = priority;
        newNode.value = val;
        newNode.father = null;
        newNode.index = this.size + 1;


        if (this.root == null) {
            assert this.size == 0;
            this.root = newNode;
            this.size = 1;
            return;
        }

        PQNode newNodeFather = this._search_node_((this.size + 1) / 2);
        if ((this.size + 1) % 2 == PQNodeTurn.LEFT.getValue()) {
            newNodeFather.left = newNode;
        } else {
            assert (this.size + 1) % 2 == PQNodeTurn.RIGHT.getValue();
            newNodeFather.right = newNode;
        }

        newNode.father = newNodeFather;
        this.size += 1;
        newNode.sift_up();
    }

    protected Pair<Comparable, Object> dequeue() throws TXLibExceptions.PQueueIsEmptyException, TXLibExceptions.PQIndexNotFound {

        if (this.root == null) {
            assert this.size == 0;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        Pair<Comparable, Object> prioValuePair = new Pair<Comparable, Object>(this.root.priority, this.root.value);

        if (this.size == 1) {
            this.root = null;
            this.size = 0;
            return prioValuePair;
        }

        PQNode swapper = this._search_node_(this.size);
        swapper.swap(this.root);

        if (this.size % 2 == PQNodeTurn.LEFT.getValue()) {
            swapper.father.left = null;
        } else {
            assert this.size % 2 == PQNodeTurn.LEFT.getValue();
            swapper.father.right = null;
        }

        this.size -= 1;
        this.root.sift_down();
        return prioValuePair;
    }
}
