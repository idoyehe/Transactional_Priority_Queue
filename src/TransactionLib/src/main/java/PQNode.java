package TransactionLib.src.main.java;


public class PQNode implements Comparable<PQNode> {
    private int index = -1;// index of the node in heap
    private Comparable priority = null;
    private Object value = null;
    private PQNode left = null;// left son heap
    private PQNode right = null;//right son heap

    public PQNode(final Comparable priority, final Object value) {
        this.setPriority(priority);
        this.setValue(value);
    }

    public PQNode(final PQNode nodeToCopy) {
        this(nodeToCopy.getPriority(), nodeToCopy.getValue());
    }


    //getters

    public PQNode getLeft() {
        return this.left;
    }

    public PQNode getRight() {
        return this.right;
    }

    public int getIndex() {
        return this.index;
    }

    public final Comparable getPriority() {
        return this.priority;
    }

    public Object getValue() {
        return this.value;
    }

    //smart setter
    void setRight(PQNode rightSon) {
        this.right = rightSon;
    }

    void setLeft(PQNode leftSon) {
        this.left = leftSon;
    }

    void setIndex(final int newIndex) {
        this.index = newIndex;
    }

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

    public int compareTo(Comparable priority) {
        return this.getPriority().compareTo(priority);
    }

    private static int[] buildDigitsArrayOfIndex(int index) {

        String binaryIndex = Integer.toBinaryString(index);
        int[] binaryDigits = new int[binaryIndex.length() - 1];

        int j = 0;
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits[j++] = Character.getNumericValue(binaryIndex.charAt(i));
        }
        return binaryDigits;
    }

    private PQNode nodeSearch(int targetIndex, int[] binaryDigits, int currentTurn) {
        if (this.getIndex() == targetIndex) {
            return this;
        }
        if (currentTurn >= binaryDigits.length) {
            return null;
        }
        if (binaryDigits[currentTurn] == PQNodeTurn.RIGHT.getValue()) {
            return this.getRight().nodeSearch(targetIndex, binaryDigits, currentTurn + 1);
        }
        assert binaryDigits[currentTurn] == PQNodeTurn.LEFT.getValue();
        return this.getLeft().nodeSearch(targetIndex, binaryDigits, currentTurn + 1);
    }

    static PQNode nodeSearch(PQNode root, int targetIndex) {
        return root.nodeSearch(targetIndex, PQNode.buildDigitsArrayOfIndex(targetIndex), 0);
    }


    PQNode nodeSiftUp(PQNode node) {
        return this.nodeSiftUp(node, PQNode.buildDigitsArrayOfIndex(node.getIndex()), 0);
    }

    private PQNode nodeSiftUp(PQNode node, int[] binaryDigits, int currentTurn) {
        assert node != null;
        if (this == node) {
            assert (currentTurn == binaryDigits.length);
            return this;
        }
        if (this.getRight() != null && binaryDigits[currentTurn] == PQNodeTurn.RIGHT.getValue()) {
            this.right = this.right.nodeSiftUp(node, binaryDigits, currentTurn + 1);
            if (this.compareTo(this.right) > 0) {// the right son is with lower priority
                PQNode leftSonHolder = this.getLeft();
                PQNode rightMinSon = this.getRight();
                int indexHolder = this.getIndex();


                this.setRight(rightMinSon.getRight());
                this.setLeft(rightMinSon.getLeft());
                this.setIndex(rightMinSon.getIndex());

                rightMinSon.setRight(this);
                rightMinSon.setLeft(leftSonHolder);
                rightMinSon.setIndex(indexHolder);
                return rightMinSon;
            }
        } else {
            assert this.getLeft() != null;//if node is a leaf stop condition should be true
            assert binaryDigits[currentTurn] == PQNodeTurn.LEFT.getValue();
            this.left = this.left.nodeSiftUp(node, binaryDigits, currentTurn + 1);
            if (this.compareTo(this.left) > 0) {// the left son is with lower priority
                PQNode rightSonHolder = this.getRight();
                PQNode leftMinSon = this.getLeft();
                int indexHolder = this.getIndex();

                this.setRight(leftMinSon.getRight());
                this.setLeft(leftMinSon.getLeft());
                this.setIndex(leftMinSon.getIndex());

                leftMinSon.setRight(rightSonHolder);
                leftMinSon.setLeft(this);
                leftMinSon.setIndex(indexHolder);
                return leftMinSon;
            }
        }
        return this;
    }

    PQNode nodeSiftDown() {
        if (this.getLeft() == null && this.getRight() == null) {// node is a leaf
            return this;
        }
        assert this.getLeft() != null; // this has at least one son, therefore it must have left son
        boolean isLeftSonSmaller = this.compareTo(this.getLeft()) > 0;
        boolean isRightSonSmaller = this.getRight() != null && this.compareTo(this.getRight()) > 0;

        if (!isLeftSonSmaller && !isRightSonSmaller) {//none of the son is smaller so done
            return this;
        }

        if (this.getRight() == null || this.getLeft().compareTo(this.getRight()) < 0) {
            PQNode rightSonHolder = this.getRight();
            PQNode leftMinSon = this.getLeft();
            int indexHolder = this.getIndex();

            this.setRight(leftMinSon.getRight());
            this.setLeft(leftMinSon.getLeft());
            this.setIndex(leftMinSon.getIndex());

            leftMinSon.setLeft(this);
            leftMinSon.setRight(rightSonHolder);
            leftMinSon.setIndex(indexHolder);

            leftMinSon.left = this.nodeSiftDown();
            return leftMinSon;
        }
        if (this.getRight() != null) {
            PQNode leftSonGolder = this.getLeft();
            PQNode rightMinSon = this.getRight();
            int indexHolder = this.getIndex();

            this.setRight(rightMinSon.getRight());
            this.setLeft(rightMinSon.getLeft());
            this.setIndex(rightMinSon.getIndex());

            rightMinSon.setRight(this);
            rightMinSon.setLeft(leftSonGolder);
            rightMinSon.setIndex(indexHolder);

            rightMinSon.right = this.nodeSiftDown();
            return rightMinSon;
        }
        return this;
    }

    public boolean isContentEqual(PQNode PQObject) {
        return this.getPriority().compareTo(PQObject.getPriority()) == 0 && this.value.equals(PQObject.value);
    }
}