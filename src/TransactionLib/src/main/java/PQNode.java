package TransactionLib.src.main.java;


public class PQNode implements Comparable<PQNode> {
    private int index = -1;// index of the node in heap
    private Comparable priority = null;
    private Object value = null;
    private PQNode left = null;// left son heap
    private PQNode right = null;//right son heap

    /**
     * New PQnode Constructor
     *
     * @param priority the priority of the new node
     * @param value    the value of new node
     */
    public PQNode(final Comparable priority, final Object value) {


        this.setPriority(priority);
        this.setValue(value);
    }

    /**
     * PQNode Copy Constructor
     *
     * @param nodeToCopy this node priority and value are copied to create new node
     */

    public PQNode(final PQNode nodeToCopy) {
        this(nodeToCopy.getPriority(), nodeToCopy.getValue());
    }


    //getters

    /**
     * @return the left son of the node, can be null
     */
    public PQNode getLeft() {
        return this.left;
    }

    /**
     * @return the right son of the node, can be null
     */
    public PQNode getRight() {
        return this.right;
    }

    /**
     * @return the index of the node, can be null
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @return the priority of the node, can be null
     */
    public final Comparable getPriority() {
        return this.priority;
    }

    /**
     * @return the value of the node, can be null
     */
    public Object getValue() {
        return this.value;
    }

    //setters

    /**
     * setter of the right son
     *
     * @param rightSon the new right son to be
     */
    void setRight(PQNode rightSon) {
        this.right = rightSon;
    }

    /**
     * setter of the left son
     *
     * @param leftSon the new left son to be
     */
    void setLeft(PQNode leftSon) {
        this.left = leftSon;
    }

    /**
     * setter of PQNode index
     *
     * @param newIndex the new index to be
     */
    void setIndex(final int newIndex) {
        this.index = newIndex;
    }

    /**
     * setter of PQNode value
     *
     * @param newValue the new value to be
     */
    void setValue(final Object newValue) {
        this.value = newValue;
    }

    /**
     * setter of PQNode priority
     *
     * @param newPriority the new priority to be
     */
    void setPriority(final Comparable newPriority) {
        this.priority = newPriority;
    }

    /**
     * Compering 2 PQNode priority
     *
     * @param pqNode node to compere to
     * @return -1 if this.priority < pqNode.priority;
     * 0 if this.priority == pqNode.priority;
     * 1 if this.priority > pqNode.priority
     */
    @Override
    public int compareTo(PQNode pqNode) {
        return this.getPriority().compareTo(pqNode.getPriority());
    }

    /**
     * Compering 2 this.priority to given priority
     *
     * @param priority to compere to
     * @return -1 if this.priority < priority;
     * 0 if this.priority == priority;
     * 1 if this.priority > priority
     */
    public int compareTo(Comparable priority) {
        return this.getPriority().compareTo(priority);
    }

    /**
     * return an index binary digits array for searching in the heap
     *
     * @param index index to be transform to binary digits
     * @return binary digits of index which index 0 is first turn, index 1 is the second turn, etc...
     */
    private static int[] buildDigitsArrayOfIndex(int index) {

        String binaryIndex = Integer.toBinaryString(index);
        int[] binaryDigits = new int[binaryIndex.length() - 1];

        int j = 0;
        for (int i = 1; i < binaryIndex.length(); i++) {
            binaryDigits[j++] = Character.getNumericValue(binaryIndex.charAt(i));
        }
        return binaryDigits;
    }

    /**
     * Searching for a node in the heap with a specific index
     *
     * @param targetIndex      the index to be found
     * @param binaryDigits     the binary digits of the index
     * @param currentTurnIndex the current turn index
     * @return a reference to the found node with the given index
     */
    private PQNode nodeSearch(int targetIndex, int[] binaryDigits, int currentTurnIndex) {
        if (this.getIndex() == targetIndex) {
            return this;
        }
        if (currentTurnIndex >= binaryDigits.length) {
            return null;
        }
        if (binaryDigits[currentTurnIndex] == PQNodeTurn.RIGHT.getValue()) {
            return this.getRight().nodeSearch(targetIndex, binaryDigits, currentTurnIndex + 1);
        }
        assert binaryDigits[currentTurnIndex] == PQNodeTurn.LEFT.getValue();
        return this.getLeft().nodeSearch(targetIndex, binaryDigits, currentTurnIndex + 1);
    }

    /**
     * Searching for a node in the heap with a specific index
     *
     * @param root        the root of the heap
     * @param targetIndex the target index to be found
     * @return a reference to the found node with the given index
     */

    static PQNode nodeSearch(PQNode root, int targetIndex) {
        return root.nodeSearch(targetIndex, PQNode.buildDigitsArrayOfIndex(targetIndex), 0);
    }

    /**
     * Sift a node up to its current location in the heap which this is the root
     *
     * @param node node to be sift
     * @return a reference to the new root of the heap
     */
    PQNode nodeSiftUp(PQNode node) {
        return this.nodeSiftUp(node, PQNode.buildDigitsArrayOfIndex(node.getIndex()), 0);
    }

    /**
     * Sift a node up to its current location in the heap which this is the root
     *
     * @param node             node to be sift
     * @param binaryDigits     the binary digits of the node index
     * @param currentTurnIndex the current turn index
     * @return a reference to the new root of the heap
     */
    private PQNode nodeSiftUp(PQNode node, int[] binaryDigits, int currentTurnIndex) {
        assert node != null;
        if (this == node) {
            assert (currentTurnIndex == binaryDigits.length);
            return this;
        }
        if (this.getRight() != null && binaryDigits[currentTurnIndex] == PQNodeTurn.RIGHT.getValue()) {
            this.right = this.right.nodeSiftUp(node, binaryDigits, currentTurnIndex + 1);
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
            assert binaryDigits[currentTurnIndex] == PQNodeTurn.LEFT.getValue();
            this.left = this.left.nodeSiftUp(node, binaryDigits, currentTurnIndex + 1);
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

    /**
     * Sift a node down to its current location in the heap which this is the root
     *
     * @return a reference to the new root of the heap
     */
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

    /**
     * Check if 2 PQNodes content is equal
     *
     * @param pqNode node to compere to
     * @return true if this.priority == pqNode.priority && this.value == this.value.equals(pqNode.value)
     * otherwise false
     */
    public boolean isContentEqual(PQNode pqNode) {
        return this.compareTo(pqNode) == 0 && this.getValue().equals(pqNode.getValue());
    }
}