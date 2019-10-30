package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

public class PQNode implements Comparable<PQNode> {
    public int index;// index of the node in heap
    public Comparable priority;
    public Object value;
    public PQNode left = null;// left son heap
    public PQNode right = null;//right son heap
    public PQNode father = null;//father heap

    @Override
    public int compareTo(PQNode pqNode) {
        return this.priority.compareTo(pqNode.priority);
    }
}