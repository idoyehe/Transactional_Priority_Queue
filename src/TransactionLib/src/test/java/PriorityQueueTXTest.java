package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static junit.framework.TestCase.*;

public class PriorityQueueTXTest {
    private static void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.father == null || node.priority.compareTo(node.father.priority) > 0;
        testHeapInvariantRecursive(node.left);
        testHeapInvariantRecursive(node.right);
    }

    @Test
    public void testPriorityQueueSingleThreadEnqueueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();

        TX.TXbegin();
        assertTrue(pQueue.isEmpty());
        IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
            pQueue.enqueue(n, n);
        });
        assertFalse(pQueue.isEmpty());
        assertNull(pQueue.internalPriorityQueue.root);
        TX.TXend();
        assertFalse(pQueue.isEmpty());
        assertEquals(new Pair<>(0, 0), pQueue.top());
        PriorityQueueTXTest.testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
    }

    @Test
    public void testPriorityQueueSingleThreadDequeueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        for (int iter = 0; iter < 2; iter++) {
            pQueue.setSingleton(false);
            TX.TXbegin();
            assertEquals(true, pQueue.isEmpty());
            try {
                pQueue.dequeue();
                assert false;
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                assert e != null;
            }
            IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
                pQueue.enqueue(n, n);
            });
            assertEquals(false, pQueue.isEmpty());
            assertEquals(null, pQueue.internalPriorityQueue.root);
            assertEquals(new Pair<>(0, 0), pQueue.top());
            IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
                try {
                    pQueue.dequeue();
                } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    assert false;
                }
            });
            assertEquals(true, pQueue.isEmpty());
            TX.TXend();
            assertEquals(true, pQueue.isEmpty());
            assertEquals(null, pQueue.internalPriorityQueue.root);
        }
    }

    @Test
    public void testPriorityQueueSingleThreadEnqueueDequeueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        final int pqueueMaxSize = 100;
        TX.TXbegin(); // 1st transcation
        assertEquals(true, pQueue.isEmpty());
        try {
            pQueue.dequeue();
            assert false;
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert e != null;
        }
        IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
            pQueue.enqueue(n, n);
        });
        assertFalse(pQueue.isEmpty());
        assertNull(pQueue.internalPriorityQueue.root);
        TX.TXend();
        assertFalse(pQueue.isEmpty());
        assertEquals(new Pair<>(0, 0), pQueue.top());
        assertEquals(pqueueMaxSize, pQueue.internalPriorityQueue.size);
        PriorityQueueTXTest.testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
        pQueue.setSingleton(false);

        TX.TXbegin(); // 2nd transcation
        assertFalse(pQueue.isEmpty());
        for (int i = 0; i < pqueueMaxSize; i++) {
            try {
                assertEquals(new Pair<>(i, i), pQueue.top());
                pQueue.dequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                assert false;
            }
        }
        assertTrue(pQueue.isEmpty());
        TX.TXend();
        assertTrue(pQueue.isEmpty());
    }

    @Test
    public void testTXPriorityQueueMultiThread() throws InterruptedException {
        final int threadsNumber = 100;
        CountDownLatch latch = new CountDownLatch(1);
        PriorityQueue pQueue = new PriorityQueue();
        Thread[] threadsARR = new Thread[threadsNumber];
        for (int i = 0; i < threadsNumber; i++) {
            threadsARR[i] = new Thread(new Run("T" + i, latch, pQueue, i * 1000));
            threadsARR[i].start();
        }
        latch.countDown();
        for (int i = 0; i < threadsNumber; i++) {
            threadsARR[i].join();

        }
        assertTrue(pQueue.isEmpty());
    }


    class Run implements Runnable {

        private PriorityQueue pQueue;
        private int priorityRef;
        private String threadName;
        private CountDownLatch latch;

        Run(String name, CountDownLatch l, PriorityQueue pq, int priorityRef) {
            this.threadName = name;
            this.latch = l;
            this.pQueue = pq;
            this.priorityRef = priorityRef;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException exp) {
                System.out.println(threadName + ": InterruptedException");
            }
            String a = threadName + "-a";
            String b = threadName + "-b";
            String c = threadName + "-c";
            String d = threadName + "-d";
            Integer p_a = 10 + this.priorityRef;
            //System.out.println(threadName + ": " + p_a);
            Integer p_b = 20 + this.priorityRef;
            //System.out.println(threadName + ": " + p_b);
            Integer p_c = 30 + this.priorityRef;
            //System.out.println(threadName + ": " + p_c);
            Integer p_d = 40 + this.priorityRef;
            //System.out.println(threadName + ": " + p_d);

            while (true) {
                try {
                    try {
                        TX.TXbegin();
                        pQueue.enqueue(p_d, d);
                        assertEquals(new Pair<>(p_d, d), pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_c, c);
                        assertEquals(new Pair<>(p_c, c), pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_b, b);
                        assertEquals(new Pair<>(p_b, b), pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_a, a);
                        assertEquals(new Pair<>(p_a, a), pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        assertEquals(new Pair<>(p_a, a), pQueue.top());
                        pQueue.dequeue();

                        assertEquals(new Pair<>(p_b, b), pQueue.top());
                        pQueue.dequeue();

                        assertEquals(new Pair<>(p_c, c), pQueue.top());
                        pQueue.dequeue();

                        assertEquals(new Pair<>(p_d, d), pQueue.top());
                        pQueue.dequeue();

                        assertTrue(pQueue.isEmpty());

                        try {
                            pQueue.top();
                            assert false;
                        } catch (TXLibExceptions.PQueueIsEmptyException exp) {
                            assert true;
                        }

                    } catch (TXLibExceptions.PQueueIsEmptyException exp) {
                        assert false;
                    } finally {
                        TX.TXend();
                    }
                } catch (TXLibExceptions.AbortException exp) {
                    continue;
                }
                break;
            }

        }
    }
}
