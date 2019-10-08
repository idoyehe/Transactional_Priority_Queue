package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.stream.IntStream;


public class LocalPriorityQueueTest {
    private void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.father == null || node.priority.compareTo(node.father.priority) > 0;
        testHeapInvariantRecursive(node.left);
        testHeapInvariantRecursive(node.right);
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
        Assert.assertEquals(3, lpq.size);
        Assert.assertEquals(2, lpq.root.left.value);
        Assert.assertEquals(3, lpq.root.right.value);

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(2, 2));
        Assert.assertEquals(2, lpq.size);

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(3, 3));
        Assert.assertEquals(1, lpq.size);

        lpq.dequeue();
        Assert.assertEquals(0, lpq.size);
        try {
            lpq.dequeue();
            assert false;
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert true;
        }

    }


    @Test
    public void testOverall() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(0, 100).forEachOrdered(n -> {
            lpq.enqueue(n, n);
        });
        testHeapInvariantRecursive(lpq.root);
        Assert.assertEquals(100, lpq.size);
        IntStream.range(0, lpq.size).forEachOrdered(n -> {
            try {
                Assert.assertEquals(new Pair<>(n, n), lpq.top());
                lpq.dequeue();

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(0, lpq.size);
    }

    @Test
    public void testKthSmallest() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(1, 101).map(i -> 1 + (101 - 1 - i)).forEach(n -> {
            lpq.enqueue(n, n);
        });
        testHeapInvariantRecursive(lpq.root);
        Assert.assertEquals(100, lpq.size);
        IntStream.range(1, 101).forEachOrdered(n -> {
            try {
                Assert.assertEquals(lpq.kThSmallest(n), new Pair<>(n, n));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(100, lpq.size);
    }
}
