package TransactionLib.src.main.java;
import java.util.ArrayList;

public class PQNode {
    protected int index;// index of the node in heap
    protected int dequeueSimulateIndex;// index of the node in heap
    protected Comparable priority;
    protected Object value;
    protected PQNode left;// left son heap
    protected PQNode right;//right son heap
    protected PQNode father;//father heap


    public void swap(PQNode node) {
        Object tempValue = node.value;
        Comparable tempPriority = node.priority;
        int tempDequeueSimulateIndex = node.dequeueSimulateIndex;
        node.value = this.value;
        node.priority = this.priority;
        node.dequeueSimulateIndex = this.dequeueSimulateIndex;
        this.value = tempValue;
        this.priority = tempPriority;
        this.dequeueSimulateIndex = tempDequeueSimulateIndex;
    }

    public void sift_up() {
        if (this.father == null || this.father.priority.compareTo(this.priority) < 0) {// case no need to sift up
            return;
        }

        this.swap(this.father);
        this.father.sift_up();
    }

    public void sift_down() {
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


    public PQNode search(int index, ArrayList<Integer> binaryDigits) throws TXLibExceptions.PQIndexNotFound {
        if (index == this.index) {
            return this;
        }
        if (binaryDigits.size() == 0) {
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQIndexNotFound();
        }

        Integer currentTurn = binaryDigits.get(0);
        binaryDigits.remove(0);
        if (binaryDigits.get(0).equals(PQNodeTurn.LEFT.getValue())) {
            return this.left.search(index, binaryDigits);
        }
        return this.right.search(index, binaryDigits);
    }
}