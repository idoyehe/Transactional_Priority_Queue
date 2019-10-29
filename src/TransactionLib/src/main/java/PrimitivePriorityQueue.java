package TransactionLib.src.main.java;

import javafx.util.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

public class PrimitivePriorityQueue {
    public PQNode root = null;
    private int size = 0;

    private static void nodesSwap(@NotNull PQNode node1, @NotNull PQNode node2) {
        Object tempValue = node1.value;
        Comparable tempPriority = node1.priority;
        node1.value = node2.value;
        node1.priority = node2.priority;
        node2.value = tempValue;
        node2.priority = tempPriority;
    }

    private static void nodeSiftUp(@NotNull PQNode node) {
        if (node.father == null || node.father.priority.compareTo(node.priority) < 0) {// case no need to sift up
            return;
        }

        PrimitivePriorityQueue.nodesSwap(node, node.father);
        PrimitivePriorityQueue.nodeSiftUp(node.father);
    }

    private static void nodeSiftDown(@NotNull PQNode node) {
        if (node.left == null && node.right == null) {// node is leaf
            return;
        }
        PQNode minSon = null;
        if (node.left != null && node.priority.compareTo(node.left.priority) > 0) {
            minSon = node.left;
        }

        if (node.right != null && node.priority.compareTo(node.right.priority) > 0) {
            if (minSon == null || minSon.priority.compareTo(node.right.priority) > 0) {
                minSon = node.right;
            }
        }
        if (minSon != null) {
            assert node.priority.compareTo(minSon.priority) > 0;
            PrimitivePriorityQueue.nodesSwap(node, minSon);
            PrimitivePriorityQueue.nodeSiftDown(minSon);

        }
    }


    private static PQNode nodeSearch(PQNode node, int index, ArrayList<Integer> binaryDigits) {
        if (index == node.index) {
            return node;
        }
        assert binaryDigits.size() > 0;

        Integer currentTurn = binaryDigits.get(0);
        binaryDigits.remove(0);
        if (currentTurn.equals(PQNodeTurn.LEFT.getValue())) {
            return PrimitivePriorityQueue.nodeSearch(node.left, index, binaryDigits);
        }
        return PrimitivePriorityQueue.nodeSearch(node.right, index, binaryDigits);
    }

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return (size <= 0);
    }

    public Pair<Comparable, Object> top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new Pair<>(this.root.priority, this.root.value);
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }


    private PQNode searchNode(int index) {
        assert 0 < index && index <= this.size;

        ArrayList<Integer> binaryDigits = new ArrayList<>();
        String binaryIndex = Integer.toBinaryString(index);
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits.add(Character.getNumericValue(binaryIndex.charAt(i)));
        }
        return PrimitivePriorityQueue.nodeSearch(this.root, index, binaryDigits);
    }


    public void enqueue(Comparable priority, Object val) {
        PQNode newNode = new PQNode();
        newNode.priority = priority;
        newNode.value = val;
        newNode.index = this.size + 1;


        if (this.root == null) {
            assert this.size == 0;
            this.root = newNode;
            this.size = 1;
            return;
        }

        PQNode newNodeFather = this.searchNode(newNode.index / 2);
        if (newNode.index % 2 == PQNodeTurn.LEFT.getValue()) {
            newNodeFather.left = newNode;
        } else {
            assert newNode.index % 2 == PQNodeTurn.RIGHT.getValue();
            newNodeFather.right = newNode;
        }

        newNode.father = newNodeFather;
        this.size += 1;
        PrimitivePriorityQueue.nodeSiftUp(newNode);
    }

    public Pair<Comparable, Object> dequeue() throws TXLibExceptions.PQueueIsEmptyException {

        if (this.root == null) {
            assert this.size == 0;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        Pair<Comparable, Object> prioValuePair = new Pair<>(this.root.priority, this.root.value);

        if (this.size == 1) {
            this.root = null;
            this.size = 0;
            return prioValuePair;
        }

        PQNode swapper = this.searchNode(this.size);
        PrimitivePriorityQueue.nodesSwap(swapper, this.root);

        if (this.size % 2 == PQNodeTurn.LEFT.getValue()) {
            swapper.father.left = null;
        } else {
            assert this.size % 2 == PQNodeTurn.RIGHT.getValue();
            swapper.father.right = null;
        }

        this.size -= 1;
        PrimitivePriorityQueue.nodeSiftDown(this.root);
        return prioValuePair;
    }
}
