package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Test;

import java.util.stream.IntStream;

import static junit.framework.TestCase.*;

public class PriorityQueueSingleThreadTest {
    final int range = 5000;

    @Test
    public void testPriorityQueueConstructor() {
        PriorityQueue pQueue = new PriorityQueue();
    }

    @Test
    public void testSingletonPriorityQueueIsEmptyWhenEmpty() {
        PriorityQueue pQueue = new PriorityQueue();
        assertTrue(pQueue.isEmpty());
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testSingletonPriorityQueueTopWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        pQueue.top();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testSingletonPriorityQueueWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        pQueue.dequeue();
    }

    @Test
    public void testSingletonPriorityQueueSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(-1, 1);
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        pQueue.enqueue(element.getKey(), element.getValue());
        assertEquals(element.getValue(), pQueue.top());
        assertEquals(1, pQueue.size());
        assertFalse(pQueue.isEmpty());
    }

    @Test
    public void testSingletonPriorityQueueSingleEnqueueAndSingleDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(-1, 1);
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        pQueue.enqueue(element.getKey(), element.getValue());
        assertEquals(element.getValue(), pQueue.top());
        assertEquals(1, pQueue.size());
        assertFalse(pQueue.isEmpty());

        assertEquals(element.getValue(), pQueue.dequeue());
        assertEquals(0, pQueue.size());
        assertTrue(pQueue.isEmpty());
    }

    @Test
    public void testSingletonPriorityQueueMultiplyEnqueueAndMultiplyDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());


        IntStream.range(0, this.range).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(n, n);
                assertEquals(element.getValue(), pQueue.top());
                assertEquals(element.getValue(), pQueue.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("PriorityQueue should not be empty");
            }
        });
    }

    @Test
    public void testSingletonPriorityQueueDecreasePriority() {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        PQObject nodesArr[] = new PQObject[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            nodesArr[n] = newRoot;
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());

        for (int i = 0; i < nodesArr.length; i++) {
            assertEquals(i, nodesArr[i].getPriority());
            assertEquals(i, nodesArr[i].getValue());
        }

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            Integer oldPrio = (Integer) nodesArr[n].getPriority();
            pQueue.decreasePriority(nodesArr[n], -oldPrio);
            assertEquals(-oldPrio, nodesArr[n].getPriority());
        });

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(-n, n);
                assertEquals(element.getValue(), pQueue.top());
                assertEquals(nodesArr[n].getValue(), pQueue.top());
                assertEquals(element.getValue(), pQueue.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
        });
    }

    @Test
    public void testTransactionalPriorityQueueIsEmptyWhenEmpty() {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        TX.TXbegin();
        assertTrue(pQueue.isEmpty());
        TX.TXend();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTransactionalPriorityQueueTopWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        TX.TXbegin();
        pQueue.top();
        TX.TXend();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTransactionalPriorityQueueWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        TX.TXbegin();
        pQueue.dequeue();
        TX.TXend();
    }

    @Test
    public void testTransactionalPriorityQueueSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(-1, 1);
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        TX.TXbegin();
        pQueue.enqueue(element.getKey(), element.getValue());
        assertEquals(element.getValue(), pQueue.top());
        assertEquals(1, pQueue.size());
        assertFalse(pQueue.isEmpty());
        TX.TXend();
        assertEquals(1, pQueue.internalPriorityQueue.size());
        assertFalse(pQueue.internalPriorityQueue.isEmpty());
    }

    @Test
    public void testTransactionalPriorityQueueSingleEnqueueAndSingleDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(-1, 1);
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        TX.TXbegin();
        pQueue.enqueue(element.getKey(), element.getValue());
        assertEquals(element.getValue(), pQueue.top());
        assertEquals(1, pQueue.size());
        assertFalse(pQueue.isEmpty());
        assertEquals(element.getValue(), pQueue.dequeue());
        assertEquals(0, pQueue.size());
        assertTrue(pQueue.isEmpty());
        TX.TXend();
        assertEquals(0, pQueue.internalPriorityQueue.size());
        assertTrue(pQueue.internalPriorityQueue.isEmpty());
    }

    @Test
    public void testTransactionalPriorityQueueEnqueueThenDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);

        TX.TXbegin();//1ST transaction only enqueue
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());
        TX.TXend();

        pQueue.setSingleton(true);
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());
        assertEquals(this.range, pQueue.internalPriorityQueue.size());
        assertFalse(pQueue.internalPriorityQueue.isEmpty());

        pQueue.setSingleton(false);
        TX.TXbegin();//2ND transaction only dequeue

        IntStream.range(0, this.range).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(n, n);
                assertEquals(element.getValue(), pQueue.top());
                assertEquals(element.getValue(), pQueue.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("PriorityQueue should not be empty");
            }
        });

        assertEquals(0, pQueue.size());
        assertTrue(pQueue.isEmpty());

        TX.TXend();
        pQueue.setSingleton(true);
        assertEquals(0, pQueue.size());
        assertTrue(pQueue.isEmpty());
        assertEquals(0, pQueue.internalPriorityQueue.size());
        assertTrue(pQueue.internalPriorityQueue.isEmpty());
    }

    @Test
    public void testTransactionalPriorityQueueEnqueueThenDequeueNotEqual() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);

        TX.TXbegin();//1ST transaction only enqueue
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());

        IntStream.range(0, this.range / 2).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(n, n);
                assertEquals(element.getValue(), pQueue.top());
                assertEquals(element.getValue(), pQueue.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("PriorityQueue should not be empty");
            }
        });

        assertEquals(this.range / 2, pQueue.size());
        assertFalse(pQueue.isEmpty());

        TX.TXend();
        pQueue.setSingleton(true);
        assertEquals(this.range / 2, pQueue.size());
        assertFalse(pQueue.isEmpty());
        assertEquals(this.range / 2, pQueue.internalPriorityQueue.size());
        assertFalse(pQueue.internalPriorityQueue.isEmpty());
    }

    @Test
    public void testTransactionalDecreasePriority() throws Exception {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        PQObject globalNodesArr[] = new PQObject[this.range];
        PQObject localNodesArr[] = new PQObject[this.range * 2];

        TX.TXbegin();//1ST transaction only enqueue
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            globalNodesArr[n] = newRoot;
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());
        TX.TXend();

        pQueue.setSingleton(true);
        assertEquals(this.range, pQueue.size());
        assertFalse(pQueue.isEmpty());

        pQueue.setSingleton(false);
        TX.TXbegin();//2ND transaction
        IntStream.range(this.range, this.range * 2).map(i -> this.range * 2 - 1 - (i - this.range)).forEach(n -> {
            final PQObject newRoot = pQueue.enqueue(n, n);
            localNodesArr[n] = newRoot;
            assertEquals(0, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range * 2, pQueue.size());
        assertFalse(pQueue.isEmpty());

        IntStream.range(0, this.range).forEach(n -> {
            assertEquals(n, globalNodesArr[n].getPriority());
            final PQObject newRoot = pQueue.decreasePriority(globalNodesArr[n], 2 * n);
            assertEquals(newRoot, globalNodesArr[n]);
            assertEquals(n, globalNodesArr[n].getPriority());
            assertEquals(n, newRoot.getPriority());
        });

        IntStream.range(0, this.range / 2).forEach(n -> {
            int newPrio = -n - 1;
            assertEquals(n, globalNodesArr[n].getPriority());
            final PQObject newRef = pQueue.decreasePriority(globalNodesArr[n], newPrio);
            assertTrue(globalNodesArr[n] != newRef);
            localNodesArr[n] = newRef;
            assertEquals(n, globalNodesArr[n].getPriority());
            assertEquals(newPrio, localNodesArr[n].getPriority());
        });


        IntStream.range(0, this.range * 2).forEach(n -> {
            if (n < this.range / 2) {
                int expectedValue = this.range / 2 - n - 1;
                try {
                    assertEquals(expectedValue, pQueue.dequeue());
                } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    fail("PriorityQueue should not be empty");
                }
            } else {
                int expectedValue = n;
                try {
                    assertEquals(expectedValue, pQueue.dequeue());
                } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    fail("PriorityQueue should not be empty");
                }
            }
        });

        TX.TXend();
        pQueue.setSingleton(true);
        
        assertEquals(0, pQueue.size());
        assertTrue(pQueue.isEmpty());
        assertEquals(0, pQueue.internalPriorityQueue.size());
        assertTrue(pQueue.internalPriorityQueue.isEmpty());
    }
}

