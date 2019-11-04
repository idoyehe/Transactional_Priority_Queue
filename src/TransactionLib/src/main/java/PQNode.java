package TransactionLib.src.main.java;

public class PQNode implements Comparable<PQNode> {
    private Comparable priority;
    private Object value;

    public PQNode(Comparable priority, Object value) {
        this.priority = priority;
        this.value = value;
    }

    public PQNode(PQNode node) {
        assert node != null;
        this.priority = node.getPriority();
        this.value = node.getValue();
    }

    //getters


    public final Comparable getPriority() {
        return this.priority;
    }

    public Object getValue() {
        return this.value;
    }

    //setters

    void setValue(final Object newValue) {
        this.value = newValue;
    }

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    @Override
    public int compareTo(PQNode pqNode) {
        return this.getPriority().compareTo(pqNode.getPriority());
    }

    public boolean isContentEqual(PQNode pqNode) {
        return this.getPriority().compareTo(pqNode.getPriority()) == 0 && this.value == value;
    }
}