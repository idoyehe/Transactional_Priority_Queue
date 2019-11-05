package TransactionLib.src.main.java;

public class PQNode implements Comparable<PQNode> {
    private long time;
    private Comparable priority;
    private Object value;

    public PQNode(Comparable priority, Object value) {
        this.priority = priority;
        this.value = value;
        this.time = 0;
    }

    public PQNode(Comparable priority, Object value, long time) {
        this(priority, value);
        this.time = time;
    }

    public PQNode(PQNode node) {
        assert node != null;
        this.priority = node.getPriority();
        this.value = node.getValue();
        this.time = node.getTime();
    }

    //getters

    public final Comparable getPriority() {
        return this.priority;
    }

    public final long getTime() {
        return this.time;
    }

    public Object getValue() {
        return this.value;
    }

    //setters

    PQNode setTime(final long newTime) {
        this.time = newTime;
        return this;
    }

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    @Override
    public int compareTo(PQNode pqNode) {
        int ret = this.getPriority().compareTo(pqNode.getPriority());
        if(ret !=0){
            return ret;
        }
        return (int)Math.signum(pqNode.getTime()-this.getTime());
    }

    public boolean isContentEqual(PQNode pqNode) {
        return this.getPriority().compareTo(pqNode.getPriority()) == 0 && this.value == value;
    }
}