package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.IntStream;

import static junit.framework.TestCase.fail;


public class LocalPriorityQueueTest {
    private void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.getFather() == null || node.compareTo(node.getFather()) > 0;
        testHeapInvariantRecursive(node.getLeft());
        testHeapInvariantRecursive(node.getRight());
    }

    @Test
    public void testLocalPriorityQueueConstructor() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
    }

    @Test
    public void testSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.enqueue(1, 1);
        Assert.assertEquals(lpq.top(), new Pair<>(1, 1));
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTopWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.top();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testDequeueWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.dequeue();
    }

    @Test
    public void testDequeue() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.enqueue(1, 1);
        lpq.enqueue(2, 2);
        lpq.enqueue(3, 3);
        Assert.assertEquals(new Pair<>(1, 1), lpq.top());
        Assert.assertEquals(3, lpq.size());
        Assert.assertEquals(2, lpq.root.getLeft().getValue());
        Assert.assertEquals(3, lpq.root.getRight().getValue());

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(2, 2));
        Assert.assertEquals(2, lpq.size());

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(3, 3));
        Assert.assertEquals(1, lpq.size());

        lpq.dequeue();
        Assert.assertEquals(0, lpq.size());
        try {
            lpq.dequeue();
            fail("Local priority queue should be empty");

        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert true;
        }

    }

    @Test
    public void testDecreasePriority() {
        final int range = 500;
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(0, range).forEachOrdered(n -> {
            n = range - 1 - n;
            PQNode newNode = lpq.enqueue(n, n);
            Assert.assertEquals(1, newNode.getIndex());
        });

        testHeapInvariantRecursive(lpq.root);

        PQNode maximumPriorityNode = lpq.enqueue(range, range);

        Assert.assertEquals(lpq.size(), maximumPriorityNode.getIndex());

        lpq.decreasePriority(maximumPriorityNode, -1);

        Assert.assertEquals(1, maximumPriorityNode.getIndex());

        try {
            Assert.assertEquals(new Pair<>(-1, range), lpq.dequeue());

        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");

        }
    }


    @Test
    public void testOverall() {
        final int range = 500;
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        PQNode nodesArray[] = new PQNode[range];
        IntStream.range(0, range).forEachOrdered(n -> {
            n = range - 1 - n;
            nodesArray[n] = lpq.enqueue(n, n);
            Assert.assertEquals(1, nodesArray[n].getIndex());
        });

        testHeapInvariantRecursive(lpq.root);

        for (int i = nodesArray.length - 1; i >= 0; i--) {
            PQNode maximumPriorityNode = nodesArray[i];
            lpq.decreasePriority(maximumPriorityNode, (int) maximumPriorityNode.getPriority() - range - 1);
            Assert.assertEquals(1, maximumPriorityNode.getIndex());
        }

        Assert.assertEquals(range, lpq.size());
        IntStream.range(0, lpq.size()).forEachOrdered(n -> {
            try {
                Assert.assertEquals(new Pair<>(n - range - 1, n), lpq.top());
                lpq.dequeue();
                testHeapInvariantRecursive(lpq.root);


            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");

            }
        });
        Assert.assertEquals(0, lpq.size());
    }

    @Test
    public void testNextSmallest() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(1, 101).map(i -> 1 + (101 - 1 - i)).forEach(n -> {
            lpq.enqueue(n, n);
        });
        testHeapInvariantRecursive(lpq.root);
        Assert.assertEquals(100, lpq.size());
        IntStream.range(1, 101).forEachOrdered(n -> {
            try {
                Assert.assertEquals(new Pair<>(n, n), lpq.currentSmallest(lpq));
                lpq.nextSmallest(lpq);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");

            }
        });
        Assert.assertEquals(100, lpq.size());
        try {
            Assert.assertEquals(new Pair<>(1, 1), lpq.currentSmallest(lpq));
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Current smallest should be Pair<>(1, 1)");

        }
        try {
            lpq.nextSmallest(lpq);
            fail("Local priority queue should be empty");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert true;
        }
    }
}
