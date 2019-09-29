package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;


public class LocalPriorityQueueTest {
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
        Assert.assertEquals(lpq.top(), new Pair<>(1, 1));
        Assert.assertEquals(lpq.size, 3);
        Assert.assertEquals(lpq.root.left.value, 2);
        Assert.assertEquals(lpq.root.right.value, 3);

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(2, 2));
        Assert.assertEquals(lpq.size, 2);

        lpq.dequeue();
        Assert.assertEquals(lpq.top(), new Pair<>(3, 3));
        Assert.assertEquals(lpq.size, 1);

        lpq.dequeue();
        Assert.assertEquals(lpq.size, 0);
        try {
            lpq.dequeue();
            assert false;
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert true;
        }

    }

    private void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.father == null || node.priority.compareTo(node.father.priority) > 0;
        testHeapInvariantRecursive(node.left);
        testHeapInvariantRecursive(node.right);
    }

    @Test
    public void testOverall() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(0, 100).forEachOrdered(n -> {
            lpq.enqueue(n, n);
        });
        testHeapInvariantRecursive(lpq.root);
        Assert.assertEquals(lpq.size, 100);
        IntStream.range(0, lpq.size).forEachOrdered(n -> {
            try {
                Assert.assertEquals(lpq.top(), new Pair<>(n, n));
                lpq.dequeue();

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(lpq.size, 0);
    }

    @Test
    public void testKthSmallest() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        IntStream.range(1, 101).map(i -> 1 + (101 - 1 - i)).forEach(n -> {
            lpq.enqueue(n, n);
        });
        testHeapInvariantRecursive(lpq.root);
        Assert.assertEquals(lpq.size, 100);
        IntStream.range(1, 101).forEachOrdered(n -> {
            try {
                Assert.assertEquals(lpq.k_th_smallest(n), new Pair<>(n, n));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(lpq.size, 100);
    }
}
