package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.ArrayList;


public class LocalPriorityQueue {
    public PQNode root = null;
    public int size = 0;
    int dequeueCounter = 0; // how many dequeue has done by the transaction
    boolean isLockedByMe = false; // is queue (not local queue) locked by me

    boolean isEmpty() {
        return (size <= 0);
    }


    public Pair<Comparable, Object> top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new Pair<>(this.root.priority, this.root.value);
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }

    private PQNode _search_node_(int index) {
        assert 0 < index && index <= this.size;

        ArrayList<Integer> binaryDigits = new ArrayList<>();
        String binaryIndex = Integer.toBinaryString(index);
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits.add(Character.getNumericValue(binaryIndex.charAt(i)));
        }
        return this.root.search(index, binaryDigits);
    }


    public void enqueue(Comparable priority, Object val) {
        PQNode newNode = new PQNode();
        newNode.priority = priority;
        newNode.value = val;
        newNode.index = this.size + 1;


        if (this.root == null) {
            assert this.size == 0;
            this.root = newNode;
            this.size = 1;
            return;
        }

        PQNode newNodeFather = this._search_node_(newNode.index / 2);
        if (newNode.index % 2 == PQNodeTurn.LEFT.getValue()) {
            newNodeFather.left = newNode;
        } else {
            assert newNode.index % 2 == PQNodeTurn.RIGHT.getValue();
            newNodeFather.right = newNode;
        }

        newNode.father = newNodeFather;
        this.size += 1;
        newNode.sift_up();
    }

    public Pair<Comparable, Object> dequeue() throws TXLibExceptions.PQueueIsEmptyException {

        if (this.root == null) {
            assert this.size == 0;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        Pair<Comparable, Object> prioValuePair = new Pair<>(this.root.priority, this.root.value);

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
            assert this.size % 2 == PQNodeTurn.RIGHT.getValue();
            swapper.father.right = null;
        }

        this.size -= 1;
        this.root.sift_down();
        return prioValuePair;
    }

    private int leftSon(int index) {
        return 2 * index;
    }

    private int rightSon(int index) {
        return 2 * index + 1;
    }

    public Pair<Comparable, Object> kThSmallest(int k) throws TXLibExceptions.PQueueIsEmptyException {

        if (k > this.size) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PriorityQueue<PQNode> lpq = new PriorityQueue<>(this.size, new PQNodeComparator());
        lpq.add(this.root);

        for (int i = 1; i < k; i++) {
            assert lpq.peek() != null;
            int topIndex = lpq.peek().index;

            try {
                lpq.remove();
            } catch (NoSuchElementException e) {
                assert false; //shouldn't be here
            }
            int leftSonIndex = this.leftSon(topIndex);
            int rightSonIndex = this.rightSon(topIndex);

            if (leftSonIndex <= this.size) {
                PQNode leftSon = this._search_node_(leftSonIndex);
                lpq.add(leftSon);
            }

            if (rightSonIndex <= this.size) {
                PQNode rightSon = this._search_node_(rightSonIndex);
                lpq.add(rightSon);
            }

        }
        assert lpq.peek() != null;
        return new Pair<>(lpq.peek().priority, lpq.peek().value);
    }
}