package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.ArrayList;

public class PrimitivePriorityQueue {
    public PQNode root = null;
    private int size = 0;

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return (size <= 0);
    }

    public Pair<Comparable, Object> top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new Pair<>(this.root.priority, this.root.value);
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }


    private PQNode searchNode(int index) {
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

        PQNode newNodeFather = this.searchNode(newNode.index / 2);
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

        PQNode swapper = this.searchNode(this.size);
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
}
