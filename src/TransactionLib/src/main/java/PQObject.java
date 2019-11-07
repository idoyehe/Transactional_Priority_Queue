package TransactionLib.src.main.java;

public class PQObject implements Comparable<PQObject> {
    private int index = -1;// index of the node in heap
    private Comparable priority = null;
    private Object value = null;

    public PQObject(Comparable priority, Object value) {
        this.priority = priority;
        this.value = value;
    }

    public PQObject(PQObject pqObject) {
        this(pqObject.getPriority(), pqObject.getValue());
    }

    //getters

    public int getIndex() {
        return this.index;
    }

    public final Comparable getPriority() {
        return this.priority;
    }

    public Object getValue() {
        return this.value;
    }


    //setters

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    void setIndex(int value) {
        this.index = value;
    }


    @Override
    public int compareTo(PQObject PQObject) {
        return this.getPriority().compareTo(PQObject.getPriority());
    }

    public int compareTo(Comparable priority) {
        return this.getPriority().compareTo(priority);
    }

    public boolean isContentEqual(PQObject PQObject) {
        return this.getPriority().compareTo(PQObject.getPriority()) == 0 && this.value.equals(PQObject.value);
    }

    // to get index of parent of node at index i
    static int parent(int i) {
        return (i - 1) / 2;
    }

    // to get index of left child of node at index i
    static int left(int i) {
        return (2 * i + 1);
    }

    // to get index of right child of node at index i
    static int right(int i) {
        return (2 * i + 2);
    }
}