package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.lang.Exception;
import java.util.ArrayList;

public class PrimitivePriorityQueue {
    protected PQNode root = null;
    private int size = 0;


    protected static PQNode nodeSiftUp(PQNode node) {
        assert node != null;
        PQNode nodeFather = node.getFather();

        if (nodeFather == null) {//in case node is the root
            return node;
        }
        if (nodeFather.compareTo(node) <= 0) {// case no need to sift up
            return PrimitivePriorityQueue.nodeSiftUp(nodeFather);
        }
        assert nodeFather.compareTo(node) > 0;

        if (node.getIndex() % 2 == PQNodeTurn.LEFT.getValue()) {//node is the LEFT son of his father
            PQNode fatherRightSonBuffer = nodeFather.getRight();
            PQNode grandFatherBuffer = nodeFather.getFather();
            int fatherIndexBuffer = nodeFather.getIndex();

            nodeFather.setRight(node.getRight());
            nodeFather.setLeft(node.getLeft());
            nodeFather.setIndex(node.getIndex());

            node.setLeft(nodeFather);//here nodeFather.father set to node
            node.setRight(fatherRightSonBuffer);
            node.setIndexAndFather(fatherIndexBuffer, grandFatherBuffer);

            return PrimitivePriorityQueue.nodeSiftUp(node);
        }

        assert node.getIndex() % 2 == PQNodeTurn.RIGHT.getValue();//node is the RIGHT son of his father
        PQNode fatherLeftSonBuffer = nodeFather.getLeft();
        PQNode grandFatherBuffer = nodeFather.getFather();
        int fatherIndexBuffer = nodeFather.getIndex();

        nodeFather.setRight(node.getRight());
        nodeFather.setLeft(node.getLeft());
        nodeFather.setIndex(node.getIndex());

        node.setRight(nodeFather);//here nodeFather.father set to node
        node.setLeft(fatherLeftSonBuffer);
        node.setIndexAndFather(fatherIndexBuffer, grandFatherBuffer);

        return PrimitivePriorityQueue.nodeSiftUp(node);
    }

    private static PQNode nodeSiftDown(PQNode node) {
        assert node != null;
        if (node.getLeft() == null && node.getRight() == null) {// node is a leaf
            return node;
        }
        assert node.getLeft() != null; // node has at least one son, therefore it must have left son
        if (node.getRight() != null && node.getRight().compareTo(node) < 0 && node.getRight().compareTo(node.getLeft()) < 0) {
            //the minimum son is the right one
            PQNode leftSonBuffer = node.getLeft();
            PQNode fatherBuffer = node.getFather();
            int indexBuffer = node.getIndex();
            PQNode minSon = node.getRight();

            node.setRight(minSon.getRight());
            node.setLeft(minSon.getLeft());
            node.setIndexAndFather(minSon.getIndex(), minSon);//here minSon.right == node

            minSon.setLeft(leftSonBuffer);
            minSon.setIndexAndFather(indexBuffer, fatherBuffer);
            minSon.setRight(PrimitivePriorityQueue.nodeSiftDown(node));
            return minSon;
        }
        if (node.compareTo(node.getLeft()) > 0) {
            PQNode rightSonBuffer = node.getRight();
            PQNode fatherBuffer = node.getFather();
            int indexBuffer = node.getIndex();
            PQNode minSon = node.getLeft();

            node.setRight(minSon.getRight());
            node.setLeft(minSon.getLeft());
            node.setIndexAndFather(minSon.getIndex(), minSon);//here minSon.left == node

            minSon.setRight(rightSonBuffer);
            minSon.setIndexAndFather(indexBuffer, fatherBuffer);
            minSon.setLeft(PrimitivePriorityQueue.nodeSiftDown(node));
            return minSon;
        }
        return node;
    }


    private static PQNode nodeSearch(PQNode node, int index, ArrayList<Integer> binaryDigits) {
        assert node != null;

        if (index == node.getIndex()) {
            return node;
        }
        assert binaryDigits.size() > 0;

        Integer currentTurn = binaryDigits.get(0);
        binaryDigits.remove(0);
        if (currentTurn.equals(PQNodeTurn.LEFT.getValue())) {
            return PrimitivePriorityQueue.nodeSearch(node.getLeft(), index, binaryDigits);
        }
        return PrimitivePriorityQueue.nodeSearch(node.getRight(), index, binaryDigits);
    }

    public int size() {
        return this.size;
    }

    public boolean isEmpty() {
        return (size <= 0);
    }

    public Pair<Comparable, Object> top() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root != null) {
            return new Pair<>(this.root.getPriority(), this.root.getValue());
        }
        TXLibExceptions excep = new TXLibExceptions();
        throw excep.new PQueueIsEmptyException();
    }


    protected PQNode findPQNode(int index) {
        if (0 >= index || index > this.size) {
            return null;
        }

        ArrayList<Integer> binaryDigits = new ArrayList<>();
        String binaryIndex = Integer.toBinaryString(index);
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits.add(Character.getNumericValue(binaryIndex.charAt(i)));
        }
        return PrimitivePriorityQueue.nodeSearch(this.root, index, binaryDigits);
    }

    final PQNode enqueueAsNode(PQNode newNode) {
        newNode.setIndex(this.size + 1);
        if (this.root == null) {
            assert this.size == 0;
            this.root = newNode;
            this.size = 1;
            return newNode;
        }

        PQNode newNodeFather = this.findPQNode(newNode.getIndex() / 2);
        if (newNode.getIndex() % 2 == PQNodeTurn.LEFT.getValue()) {
            newNodeFather.setLeft(newNode);
        } else {
            assert newNode.getIndex() % 2 == PQNodeTurn.RIGHT.getValue();
            newNodeFather.setRight(newNode);
        }

        this.size += 1;
        this.root = PrimitivePriorityQueue.nodeSiftUp(newNode);
        return newNode;
    }


    public final PQNode enqueue(Comparable priority, Object value) {
        PQNode newNode = new PQNode();
        newNode.setPriority(priority);
        newNode.setValue(value);
        return this.enqueueAsNode(newNode);
    }

    public void decreasePriority(final PQNode nodeToModify, Comparable newPriority) {
        assert this.containsNode(nodeToModify);//checking node is actually part of the heap
        if (nodeToModify.getPriority().compareTo(newPriority) > 0) {
            nodeToModify.setPriority(newPriority);
            this.root = PrimitivePriorityQueue.nodeSiftUp(nodeToModify);
        }
    }

    PQNode dequeueAsNode() throws TXLibExceptions.PQueueIsEmptyException {
        if (this.root == null) {
            assert this.size == 0;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new PQueueIsEmptyException();
        }
        PQNode nodeToDequeue = this.root;

        if (this.size == 1) {
            this.root = null;
            this.size = 0;
            return nodeToDequeue;
        }

        PQNode maxIndexNode = this.findPQNode(this.size);

        if (this.size % 2 == PQNodeTurn.LEFT.getValue()) {
            maxIndexNode.getFather().setLeft(null);
        } else {
            assert this.size % 2 == PQNodeTurn.RIGHT.getValue();
            maxIndexNode.getFather().setRight(null);
        }
        maxIndexNode.setIndexAndFather(1, null);
        maxIndexNode.setRight(this.root.getRight());
        maxIndexNode.setLeft(this.root.getLeft());

        this.root.setLeft(null);
        this.root.setRight(null);
        assert this.root.getFather() == null;

        this.size -= 1;
        this.root = maxIndexNode;
        this.root = PrimitivePriorityQueue.nodeSiftDown(this.root);
        return nodeToDequeue;
    }

    public Pair<Comparable, Object> dequeue() throws TXLibExceptions.PQueueIsEmptyException {
        PQNode dequeuedNode = this.dequeueAsNode();
        return new Pair<Comparable, Object>(dequeuedNode.getPriority(), dequeuedNode.getValue());
    }

    boolean containsNode(PQNode node) {
        return this.findPQNode(node.getIndex()) == node;
    }

    private static void testHeapInvariantRecursiveAUX(PQNode root) throws Exception {
        if (root == null) {
            return;
        }
        if (root.getFather() != null && root.compareTo(root.getFather()) <= 0) {
            throw new Exception("Heap invariant is violated");
        }
        if (root.getFather() != null && root.getIndex() / 2 != root.getFather().getIndex()) {
            throw new Exception("Heap invariant is violated");
        }

        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(root.getLeft());
        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(root.getRight());
    }

    public void testHeapInvariantRecursive() throws Exception {
        PrimitivePriorityQueue.testHeapInvariantRecursiveAUX(this.root);
    }

}