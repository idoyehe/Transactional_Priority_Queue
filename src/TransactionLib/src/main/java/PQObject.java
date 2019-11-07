package TransactionLib.src.main.java;

public class PQObject implements Comparable<PQObject> {
    private int index = -1;// index of the node in heap
    private Comparable priority = null;
    private Object value = null;
    private boolean _isIgnored = false;

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

    public boolean getIsIgnored() {
        return this._isIgnored;
    }


    //setters

    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    public void setIgnored() {
        assert !this._isIgnored;
        this._isIgnored = true;
    }

    @Override
    public int compareTo(PQObject PQObject) {
        return this.getPriority().compareTo(PQObject.getPriority());
    }

    public boolean isContentEqual(PQObject PQObject) {
        return this.getPriority().compareTo(PQObject.getPriority()) == 0 && this.value.equals(PQObject.value);
    }
}