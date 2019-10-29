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
}

class PQNodeComparator implements Comparator<PQNode> {
    public int compare(PQNode pqn1, PQNode pqn2) {
        return pqn1.priority.compareTo(pqn2.priority);
    }
}