package TransactionLib.src.main.java;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class LocalStorage {

    long readVersion = 0L;
    public long writeVersion = 0L; // for debug//TODO:change back to protected
    protected boolean TX = false;// this flag indicate for transaction
    boolean readOnly = true;
    HashMap<Queue, LocalQueue> queueMap = new HashMap<Queue, LocalQueue>();
    HashMap<PriorityQueue, LocalPriorityQueue> priorityQueueMap = new HashMap<PriorityQueue, LocalPriorityQueue>();
    HashMap<LNode, WriteElement> writeSet = new HashMap<LNode, WriteElement>();//this is the writing set (from paper)
    HashSet<LNode> readSet = new HashSet<LNode>();//this is the reading set (from paper)
    HashMap<LinkedList, ArrayList<LNode>> indexAdd = new HashMap<LinkedList, ArrayList<LNode>>();//index handling
    HashMap<LinkedList, ArrayList<LNode>> indexRemove = new HashMap<LinkedList, ArrayList<LNode>>();//index handling
    // with ArrayList all nodes will be added to the list
    // (no compression needed)
    // later, when we add the nodes to the index,
    // the latest node that was added to this list
    // will be the last update to the node in the index

    void putIntoWriteSet(LNode node, LNode next, Object val, boolean deleted) {
        WriteElement we = new WriteElement();
        we.next = next;
        we.deleted = deleted;
        we.val = val;
        writeSet.put(node, we);//is node is it's prev node of the new node?
    }

    void addToIndexAdd(LinkedList list, LNode node) {
        ArrayList<LNode> nodes = indexAdd.get(list);
        if (nodes == null) {
            nodes = new ArrayList<LNode>();
        }
        nodes.add(node);
        indexAdd.put(list, nodes);
    }


    protected void addToIndexRemove(LinkedList list, LNode node) {
        ArrayList<LNode> nodes = indexRemove.get(list);
        if (nodes == null) {
            nodes = new ArrayList<LNode>();
        }
        nodes.add(node);
        indexRemove.put(list, nodes);
    }

}
