package TransactionLib.src.main.java;

public class PQObject implements Comparable<PQObject> {
    private int index = Integer.MAX_VALUE;// index of the node in heap
    private Comparable priority;
    private Object value;

    /**
     * New PQObject Constructor
     *
     * @param priority the priority of the new node
     * @param value    the value of new node
     */
    public PQObject(Comparable priority, Object value) {
        this.priority = priority;
        this.value = value;
    }

    /**
     * PQObject Copy Constructor
     *
     * @param pqObject this node priority and value are copied to create new node
     */
    public PQObject(PQObject pqObject) {
        this(pqObject.getPriority(), pqObject.getValue());
    }

    //getters

    /**
     * @return the index of the node, can be null
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @return the priority of the node, can be null
     */
    public final Comparable getPriority() {
        return this.priority;
    }

    /**
     * @return the value of the node, can be null
     */
    public Object getValue() {
        return this.value;
    }


    //setters

    /**
     * setter of PQNode priority
     *
     * @param newPriority the new priority to be
     */
    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    /**
     * setter of PQNode index
     *
     * @param newIndex the new index to be
     */
    void setIndex(int newIndex) {
        this.index = newIndex;
    }

    /**
     * Compering 2 PQObject priority
     *
     * @param pqObject node to compere to
     * @return -1 if this.priority < pqNode.priority;
     * 0 if this.priority == pqNode.priority;
     * 1 if this.priority > pqNode.priority
     */
    @Override
    public int compareTo(PQObject pqObject) {
        return this.getPriority().compareTo(pqObject.getPriority());
    }

    /**
     * Compering this.priority to given priority
     *
     * @param priority to compere to
     * @return -1 if this.priority < priority;
     * 0 if this.priority == priority;
     * 1 if this.priority > priority
     */
    public int compareTo(Comparable priority) {
        return this.getPriority().compareTo(priority);
    }

    /**
     * Check if 2 PQNodes content is equal
     *
     * @param pqObject node to compere to
     * @return true if this.priority == pqNode.priority && this.value == this.value.equals(pqNode.value)
     * otherwise false
     */
    public boolean isContentEqual(PQObject pqObject) {
        return this.compareTo(pqObject) == 0 && this.value.equals(pqObject.value);
    }

    /**
     * @param index
     * @return parent index
     */
    static int parent(int index) {
        return (index - 1) / 2;
    }

    /**
     * @param index
     * @return left son index
     */
    static int left(int index) {
        return (2 * index + 1);
    }

    /**
     * @param index
     * @return right son index
     */
    static int right(int index) {
        return (2 * index + 2);
    }
}