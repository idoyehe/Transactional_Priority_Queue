package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

public class PQNode {
    public int index;// index of the node in heap
    public Comparable priority;
    public Object value;
    public PQNode left = null;// left son heap
    public PQNode right = null;//right son heap
    public PQNode father = null;//father heap


    void swap(PQNode node) {
        Object tempValue = node.value;
        Comparable tempPriority = node.priority;
        node.value = this.value;
        node.priority = this.priority;
        this.value = tempValue;
        this.priority = tempPriority;
    }

    void sift_up() {
        if (this.father == null || this.father.priority.compareTo(this.priority) < 0) {// case no need to sift up
            return;
        }

        this.swap(this.father);
        this.father.sift_up();
    }

    void sift_down() {
        if (this.left == null && this.right == null) {// node is leaf
            return;
        }


        PQNode min_son = null;
        if (this.left != null && this.priority.compareTo(this.left.priority) > 0) {
            min_son = this.left;
        }

        if (this.right != null && this.priority.compareTo(this.right.priority) > 0) {
            if (min_son == null || min_son.priority.compareTo(this.right.priority) > 0) {
                min_son = this.right;

            }
        }
        if (min_son != null) {
            assert this.priority.compareTo(min_son.priority) > 0;
            this.swap(min_son);
            min_son.sift_down();

        }
    }


    public PQNode search(int index, ArrayList<Integer> binaryDigits) {
        if (index == this.index) {
            return this;
        }
        assert binaryDigits.size() > 0;

        Integer currentTurn = binaryDigits.get(0);
        binaryDigits.remove(0);
        if (currentTurn.equals(PQNodeTurn.LEFT.getValue())) {
            return this.left.search(index, binaryDigits);
        }
        return this.right.search(index, binaryDigits);
    }
}

class PQNodeComparator implements Comparator<PQNode> {
    public int compare(PQNode pqn1, PQNode pqn2) {
        return pqn1.priority.compareTo(pqn2.priority);
    }
}