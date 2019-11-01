package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static junit.framework.TestCase.*;

public class PriorityQueueTXTest {
    private static void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.getFather() == null || node.compareTo(node.getFather()) > 0;
        testHeapInvariantRecursive(node.getLeft());
        testHeapInvariantRecursive(node.getRight());
    }

    @Test
    public void testPriorityQueueSingleThreadEnqueueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        final int range = 100;
        TX.TXbegin();
        assertTrue(pQueue.isEmpty());
        IntStream.range(0, range).map(i -> 100 - 1 - i).forEach(n -> {
            PQNode newNode = pQueue.enqueue(n, n);
            assertEquals(1, newNode.getIndex());
        });


        PQNode maximumPriorityNode = pQueue.enqueue(range, range);
        assertEquals(range + 1, maximumPriorityNode.getIndex());
        pQueue.decreasePriority(maximumPriorityNode, -1);
        assertEquals(1, maximumPriorityNode.getIndex());

        try {
            assertEquals(range, pQueue.dequeue());

        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");
        }

        assertFalse(pQueue.isEmpty());
        assertNull(pQueue.internalPriorityQueue.root);
        TX.TXend();
        assertFalse(pQueue.isEmpty());
        assertEquals(0, pQueue.top());
        PriorityQueueTXTest.testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
    }

    @Test
    public void testPriorityQueueSingleThreadDequeueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        final int range = 100;
        PriorityQueue pQueue = new PriorityQueue();
        for (int iter = 0; iter < 2; iter++) {
            pQueue.setSingleton(false);
            TX.TXbegin();
            assertEquals(true, pQueue.isEmpty());
            try {
                pQueue.dequeue();
                fail("Local priority queue should be empty");

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                assert e != null;
            }
            IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
                pQueue.enqueue(n, n);
            });

            PQNode maximumPriorityNode = pQueue.enqueue(range, range);
            assertEquals(range + 1, maximumPriorityNode.getIndex());
            pQueue.decreasePriority(maximumPriorityNode, -1);
            assertEquals(1, maximumPriorityNode.getIndex());

            try {
                assertEquals(range, pQueue.dequeue());

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }


            assertEquals(false, pQueue.isEmpty());
            assertEquals(null, pQueue.internalPriorityQueue.root);
            assertEquals(0, pQueue.top());
            IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
                try {
                    pQueue.dequeue();
                } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    fail("Local priority queue should not be empty");
                }
            });
            TX.TXend();

            TX.TXbegin();
            assertEquals(true, pQueue.isEmpty());
            try {
                pQueue.dequeue();
                fail("Local priority queue should be empty");
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                assert e != null;
            }
            TX.TXend();

            assertEquals(true, pQueue.isEmpty());
            try {
                pQueue.dequeue();
                fail("Local priority queue should be empty");
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                assert e != null;
            }
            assertEquals(null, pQueue.internalPriorityQueue.root);
        }
    }

    @Test
    public void testPriorityQueueSingleThreadEnqueueDequeueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        final int range = 100;

        TX.TXbegin(); // 1st transcation
        assertEquals(true, pQueue.isEmpty());
        try {
            pQueue.dequeue();
            fail("Local priority queue should be empty");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert e != null;
        }
        IntStream.range(0, 100).map(i -> 100 - 1 - i).forEach(n -> {
            pQueue.enqueue(n, n);
        });
        PQNode maximumPriorityNode = pQueue.enqueue(range, range);
        assertEquals(range + 1, maximumPriorityNode.getIndex());
        pQueue.decreasePriority(maximumPriorityNode, -1);
        assertEquals(1, maximumPriorityNode.getIndex());

        assertFalse(pQueue.isEmpty());
        assertNull(pQueue.internalPriorityQueue.root);
        TX.TXend();


        assertFalse(pQueue.isEmpty());
        assertEquals(range, pQueue.top());
        assertEquals(range + 1, pQueue.internalPriorityQueue.size());
        PriorityQueueTXTest.testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
        pQueue.setSingleton(false);

        TX.TXbegin(); // 2nd transaction
        assertFalse(pQueue.isEmpty());

        try {
            assertEquals(range, pQueue.dequeue());

        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");
        }

        for (int i = 0; i < range; i++) {
            try {
                assertEquals(i, pQueue.top());
                pQueue.dequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("Local priority queue should not be empty");
            }
        }
        assertTrue(pQueue.isEmpty());
        try {
            pQueue.dequeue();
            fail("Local priority queue should be empty");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert e != null;
        }
        TX.TXend();
        assertTrue(pQueue.isEmpty());
        try {
            pQueue.dequeue();
            fail("Local priority queue should be empty");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assert e != null;
        }
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
                        PQNode maximumPriorityNode = pQueue.enqueue(p_d, d);
                        assertEquals(d, pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_c, c);
                        assertEquals(c, pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_b, b);
                        assertEquals(b, pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.enqueue(p_a, a);
                        assertEquals(a, pQueue.top());
                        assertFalse(pQueue.isEmpty());

                        pQueue.decreasePriority(maximumPriorityNode, this.priorityRef);
                        assertEquals(d, pQueue.top());
                        pQueue.dequeue();

                        assertEquals(a, pQueue.top());
                        pQueue.dequeue();

                        assertEquals(b, pQueue.top());
                        pQueue.dequeue();

                        assertEquals(c, pQueue.top());
                        pQueue.dequeue();

                        assertTrue(pQueue.isEmpty());

                        try {
                            pQueue.top();
                            fail("Local priority queue should be empty");
                        } catch (TXLibExceptions.PQueueIsEmptyException exp) {
                            assert true;
                        }
                        try {
                            pQueue.dequeue();
                            fail("Local priority queue should be empty");
                        } catch (TXLibExceptions.PQueueIsEmptyException e) {
                            assert e != null;
                        }

                    } catch (TXLibExceptions.PQueueIsEmptyException exp) {
                        fail("Local priority queue should not be empty");
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

