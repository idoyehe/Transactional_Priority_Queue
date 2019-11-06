package TransactionLib.src.main.java;

public class PQObject implements Comparable<PQObject> {
    private long ref;
    private long time;
    private Comparable priority;
    private Object value;
    private boolean isIgnored = false;

    public PQObject(Comparable priority, Object value) {
        this.priority = priority;
        this.value = value;
        this.time = 0;
    }

    public PQObject(Comparable priority, Object value, long time) {
        this(priority, value);
        this.time = time;
    }

    public PQObject(PQObject node) {
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

    public boolean getIsIgnored() {
        return this.isIgnored;
    }

    public long getRef() {
        return this.ref;
    }

    //setters

    PQObject setTime(final long newTime) {
        this.time = newTime;
        return this;
    }

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    void setRef(long ref) {
        this.ref = ref;
    }

    public void setIgnored() {
        assert !this.isIgnored;
        this.isIgnored = true;
    }

    @Override
    public int compareTo(PQObject pqNode) {
        int ret = this.getPriority().compareTo(pqNode.getPriority());
        if (ret != 0) {
            return ret;
        }
        return (int) Math.signum(pqNode.getTime() - this.getTime());
    }

    public boolean isContentEqual(PQObject pqNode) {
        return this.getPriority().compareTo(pqNode.getPriority()) == 0 && this.value.equals(pqNode.value);
    }
}