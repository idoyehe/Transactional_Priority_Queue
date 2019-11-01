package TransactionLib.src.main.java;

public class PQNode implements Comparable<PQNode> {
    private int index;// index of the node in heap
    private Comparable priority;
    private Object value;
    private PQNode left = null;// left son heap
    private PQNode right = null;//right son heap
    private PQNode father = null;//father heap

    //getters

    public PQNode getFather() {
        return this.father;
    }

    public PQNode getLeft() {
        return this.left;
    }

    public PQNode getRight() {
        return this.right;
    }

    public int getIndex() {
        return this.index;
    }

    public final Comparable getPriority() {
        return this.priority;
    }

    public Object getValue() {
        return this.value;
    }

    //smart setter
    void setRight(PQNode rightSon) {
        this.right = rightSon;
        if (this.right != null) {
            this.right.father = this;
        }
    }

    void setLeft(PQNode leftSon) {
        this.left = leftSon;
        if (this.left != null) {
            this.left.father = this;
        }
    }

    void setFatherAndIndex(int index, PQNode newFather) {
        this.setIndex(index);
        this.father = newFather;
        if (this.father != null) {
            if (this.getIndex() % 2 == PQNodeTurn.LEFT.getValue()) {
                this.father.left = this;
            } else {
                assert this.getIndex() % 2 == PQNodeTurn.RIGHT.getValue();
                this.father.right = this;
            }
        }
    }

    void setIndex(final int newIndex) {
        this.index = newIndex;
    }

    void setValue(final Object newValue) {
        this.value = newValue;
    }

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    @Override
    public int compareTo(PQNode pqNode) {//TODO>:workaround
        return (this.getPriority().compareTo(pqNode.getPriority()) <= 0) ? -1 : 1;
    }
}