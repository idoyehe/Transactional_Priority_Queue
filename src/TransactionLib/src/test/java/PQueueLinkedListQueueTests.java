package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class PQueueLinkedListQueueTests {

    private static final int ITERATIONS = 100;
    private static final int RANGE = 10000;
    private static final int THREADS = 10;

    @Test
    public void testSingleThreadTX() throws TXLibExceptions.AbortException, TXLibExceptions.QueueIsEmptyException, TXLibExceptions.PQueueIsEmptyException {

        Queue Q = new Queue();
        PriorityQueue PQ = new PriorityQueue();
        LinkedList LL = new LinkedList();

        Assert.assertEquals(true, Q.isEmpty());
        Assert.assertEquals(true, PQ.isEmpty());
        Q.enqueue(1);
        PQ.enqueue(10, 10);
        Q.enqueue(2);
        PQ.enqueue(20, 20);
        Q.enqueue(3);
        PQ.enqueue(30, 30);
        Q.enqueue(13);
        PQ.enqueue(130, 130);
        Q.enqueue(12);
        PQ.enqueue(120, 120);
        Q.enqueue(11);
        PQ.enqueue(110, 110);
        Assert.assertEquals(false, Q.isEmpty());
        Assert.assertEquals(false, PQ.isEmpty());

        Assert.assertEquals(null, LL.put(1, Q.dequeue()));
        Assert.assertEquals(true, LL.containsKey(1));
        Assert.assertEquals(null, LL.put(10, PQ.dequeue()));
        Assert.assertEquals(true, LL.containsKey(10));
        Assert.assertEquals(1, LL.get(1));
        Assert.assertEquals(new Pair<>(10, 10), LL.get(10));
        Assert.assertEquals(null, LL.put(2, Q.dequeue()));
        Assert.assertEquals(null, LL.put(3, Q.dequeue()));
        Assert.assertEquals(null, LL.put(20, PQ.dequeue()));
        Assert.assertEquals(null, LL.put(30, PQ.dequeue()));
        Assert.assertEquals(3, LL.get(3));
        Assert.assertEquals(2, LL.get(2));
        Assert.assertEquals(new Pair<>(30, 30), LL.get(30));
        Assert.assertEquals(new Pair<>(20, 20), LL.get(20));

        Q.enqueue(40);
        PQ.enqueue(100, 100);
        Assert.assertEquals(null, LL.put(13, Q.dequeue()));
        Assert.assertEquals(null, LL.put(12, Q.dequeue()));
        Assert.assertEquals(null, LL.put(11, Q.dequeue()));
        Assert.assertEquals(null, LL.put(40, Q.dequeue()));
        Assert.assertEquals(null, LL.put(100, PQ.dequeue()));
        Assert.assertEquals(null, LL.put(110, PQ.dequeue()));
        Assert.assertEquals(null, LL.put(120, PQ.dequeue()));
        Assert.assertEquals(null, LL.put(130, PQ.dequeue()));

        try {
            Assert.assertEquals(null, LL.put(0, Q.dequeue()));
            Assert.fail("did not throw QueueIsEmptyException");
        } catch (TXLibExceptions.QueueIsEmptyException ignored) {
        }
        try {
            Assert.assertEquals(null, LL.put(0, PQ.dequeue()));
            Assert.fail("did not throw PQueueIsEmptyException");
        } catch (TXLibExceptions.PQueueIsEmptyException ignored) {
        }

        Assert.assertEquals(40, LL.get(40));
        Assert.assertEquals(11, LL.get(11));
        Assert.assertEquals(13, LL.get(13));
        Assert.assertEquals(12, LL.get(12));

        Assert.assertEquals(new Pair<>(100, 100), LL.get(100));
        Assert.assertEquals(new Pair<>(110, 110), LL.get(110));
        Assert.assertEquals(new Pair<>(120, 120), LL.get(120));
        Assert.assertEquals(new Pair<>(130, 130), LL.get(130));


        while (true) {
            try {
                try {
                    TX.TXbegin();
                    Assert.assertEquals(true, Q.isEmpty());
                    Assert.assertEquals(true, PQ.isEmpty());
                    Q.enqueue(LL.remove(13));
                    Q.enqueue(LL.remove(12));
                    Q.enqueue(LL.remove(11));
                    Q.enqueue(LL.remove(40));
                    Q.enqueue(LL.remove(1));
                    Q.enqueue(LL.remove(2));
                    Q.enqueue(LL.remove(3));

                    Pair<Comparable, Object> element = (Pair<Comparable, Object>) LL.remove(130);
                    PQ.enqueue(element.getKey(), element.getValue());

                    element = (Pair<Comparable, Object>) LL.remove(120);
                    PQ.enqueue(element.getKey(), element.getValue());

                    element = (Pair<Comparable, Object>) LL.remove(110);
                    PQ.enqueue(element.getKey(), element.getValue());

                    element = (Pair<Comparable, Object>) LL.remove(100);
                    PQ.enqueue(element.getKey(), element.getValue());

                    element = (Pair<Comparable, Object>) LL.remove(10);
                    PQ.enqueue(element.getKey(), element.getValue());
                    element = (Pair<Comparable, Object>) LL.remove(20);
                    PQ.enqueue(element.getKey(), element.getValue());

                    element = (Pair<Comparable, Object>) LL.remove(30);
                    PQ.enqueue(element.getKey(), element.getValue());
                    Assert.assertEquals(false, LL.containsKey(13));
                    Assert.assertEquals(false, LL.containsKey(40));
                    Assert.assertEquals(false, LL.containsKey(130));
                    Assert.assertEquals(false, LL.containsKey(100));
                    Assert.assertEquals(false, Q.isEmpty());
                    Assert.assertEquals(false, PQ.isEmpty());
                    Assert.assertEquals(new Pair<>(10, 10), PQ.top());
                } finally {
                    TX.TXend();
                }
            } catch (TXLibExceptions.AbortException exp) {
                continue;
            }
            break;
        }

        Assert.assertEquals(false, Q.isEmpty());
        Q.dequeue();
        Q.dequeue();
        Q.dequeue();
        Q.dequeue();
        Q.dequeue();
        Q.dequeue();
        Q.dequeue();

        Assert.assertEquals(new Pair<>(10, 10), PQ.dequeue());
        Assert.assertEquals(new Pair<>(20, 20), PQ.dequeue());
        Assert.assertEquals(new Pair<>(30, 30), PQ.dequeue());
        Assert.assertEquals(new Pair<>(100, 100), PQ.dequeue());
        Assert.assertEquals(new Pair<>(110, 110), PQ.dequeue());
        Assert.assertEquals(new Pair<>(120, 120), PQ.dequeue());
        Assert.assertEquals(new Pair<>(130, 130), PQ.dequeue());

        try {
            Q.dequeue();
            Assert.fail("did not throw PQueueIsEmptyException");
        } catch (TXLibExceptions.QueueIsEmptyException ignored) {
        }
        try {
            PQ.dequeue();
            Assert.fail("did not throw QueueIsEmptyException");
        } catch (TXLibExceptions.PQueueIsEmptyException ignored) {
        }
    }

    @Test
    public void testMultiThreadSimpleTransaction() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        LinkedList LL = new LinkedList();
        Queue Q = new Queue();
        PriorityQueue PQ = new PriorityQueue();

        ArrayList<Thread> threads = new ArrayList<>(THREADS);
        for (int i = 0; i < THREADS; i++) {
            threads.add(new Thread(new RunSimpleTransaction(latch, LL, Q, PQ)));
        }
        for (int i = 0; i < THREADS; i++) {
            threads.get(i).start();
        }
        latch.countDown();
        for (int i = 0; i < THREADS; i++) {
            threads.get(i).join();
        }
    }

    class RunSimpleTransaction implements Runnable {

        LinkedList LL;
        Queue Q;
        PriorityQueue PQ;
        CountDownLatch latch;

        RunSimpleTransaction(CountDownLatch l, LinkedList ll, Queue q, PriorityQueue pq) {
            latch = l;
            LL = ll;
            Q = q;
            PQ = pq;
        }

        @Override
        public void run() {
            try {
                latch.await();
            } catch (InterruptedException exp) {
                System.out.println("InterruptedException");
            }
            for (int i = 0; i < ITERATIONS; i++) {

                Random rand = new Random();
                int n = rand.nextInt((RANGE) + 1);

                while (true) {
                    try {
                        try {
                            TX.TXbegin();
                            LL.put(n, n);

                            LL.put(n + 3, n + 3);
                            LL.put(n + 6, n + 6);
                            LL.put(n + 9, n + 9);
                            LL.put(n + 12, n + 12);
                            LL.put(n + 15, n + 15);
                            LL.put(n + 7, n + 7);
                            LL.put(n + 11, n + 11);
                            assertEquals(true, LL.containsKey(n + 12));
                            assertEquals(n + 12, LL.get(n + 12));
                            assertEquals(true, LL.containsKey(n + 15));
                            assertEquals(n + 15, LL.get(n + 15));
                            assertEquals(n, LL.get(n));
                            assertEquals(n + 11, LL.get(n + 11));
                            assertEquals(n + 7, LL.get(n + 7));
                            LL.put(n, n);

                            LL.put(n + 2, n + 2);
                            LL.put(n + 4, n + 4);
                            LL.put(n + 6, n + 6);
                            LL.put(n + 10, n + 10);
                            LL.put(n + 14, n + 14);
                            LL.put(n + 8, n + 8);
                            LL.put(n + 16, n + 16);
                            assertEquals(true, LL.containsKey(n + 10));
                            assertEquals(n + 10, LL.get(n + 10));
                            assertEquals(true, LL.containsKey(n + 14));
                            assertEquals(n + 14, LL.get(n + 14));
                            assertEquals(n, LL.get(n));
                            assertEquals(n + 16, LL.get(n + 16));
                            assertEquals(n + 4, LL.get(n + 4));
                            assertEquals(n + 2, LL.get(n + 2));

                            Q.enqueue(LL.remove(n + 7));
                            Q.enqueue(LL.remove(n + 9));
                            Q.enqueue(LL.remove(n + 11));

                            PQ.enqueue(n + 6, LL.remove(n + 6));
                            PQ.enqueue(n + 10, LL.remove(n + 10));
                            PQ.enqueue(n + 14, LL.remove(n + 14));

                            assertEquals(false, Q.isEmpty());
                            assertEquals(false, PQ.isEmpty());


                            assertEquals(null, LL.put(n + 7, Q.dequeue()));
                            assertEquals(null, LL.put(n + 9, Q.dequeue()));
                            assertEquals(false, Q.isEmpty());

                            assertEquals(new Pair<>(n + 6, n + 6), PQ.top());
                            assertEquals(null, LL.put(n + 6, PQ.dequeue().getValue()));
                            assertEquals(new Pair<>(n + 10, n + 10), PQ.top());
                            assertEquals(null, LL.put(n + 10, PQ.dequeue().getValue()));
                            assertEquals(false, PQ.isEmpty());

                            Q.enqueue(LL.remove(n + 15));
                            PQ.enqueue(n + 16, LL.remove(n + 16));

                            assertEquals(null, LL.put(n + 11, Q.dequeue()));
                            assertEquals(null, LL.put(n + 15, Q.dequeue()));
                            assertEquals(true, Q.isEmpty());


                            assertEquals(new Pair<>(n + 14, n + 14), PQ.top());
                            assertEquals(null, LL.put(n + 14, PQ.dequeue().getValue()));
                            assertEquals(new Pair<>(n + 16, n + 16), PQ.top());
                            assertEquals(null, LL.put(n + 16, PQ.dequeue().getValue()));
                            assertEquals(true, PQ.isEmpty());


                            assertEquals(true, LL.containsKey(n + 12));
                            assertEquals(n + 12, LL.get(n + 12));
                            assertEquals(true, LL.containsKey(n + 15));
                            assertEquals(n + 15, LL.get(n + 15));

                            assertEquals(true, LL.containsKey(n + 8));
                            assertEquals(n + 8, LL.get(n + 8));
                            assertEquals(true, LL.containsKey(n + 16));
                            assertEquals(n + 16, LL.get(n + 16));

                            assertEquals(n, LL.get(n));
                            assertEquals(n + 11, LL.get(n + 11));
                            assertEquals(n + 7, LL.get(n + 7));

                            assertEquals(n + 10, LL.get(n + 10));
                            assertEquals(n + 6, LL.get(n + 6));


                        } catch (TXLibExceptions.QueueIsEmptyException exp) {
                            fail("Queue should not be empty");

                        } catch (TXLibExceptions.PQueueIsEmptyException exp) {
                            fail("Priority Queue should not be empty");
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
}
