package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;

public class PriorityQueueSingletonTest {
    static void testHeapInvariantRecursive(PQNode node) {
        if (node == null) {
            return;
        }
        assert node.father == null || node.priority.compareTo(node.father.priority) > 0;
        testHeapInvariantRecursive(node.left);
        testHeapInvariantRecursive(node.right);
    }

    @Test
    public void testTXPriorityQueueSingleton() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();
        Integer zero = 0;
        Assert.assertEquals(true, pQueue.isEmpty());
        try {
            pQueue.top();
            assert false;
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
//            e.printStackTrace();
            assert true;
        }
        Assert.assertEquals(true, pQueue.isEmpty());

        String zeroS = "zero";
        pQueue.enqueue(zero, zeroS);
        Assert.assertEquals(new Pair<>(zero, zeroS), pQueue.top());
        Assert.assertEquals(new Pair<>(zero, zeroS), pQueue.dequeue());

        Integer one = 1;
        String oneS = "one";
        Integer two = 2;
        String twoS = "two";
        pQueue.enqueue(two, twoS);
        pQueue.enqueue(one, oneS);
        Assert.assertEquals(new Pair<>(one, oneS), pQueue.dequeue());
        Assert.assertEquals(new Pair<>(two, twoS), pQueue.dequeue());
        Assert.assertEquals(true, pQueue.isEmpty());


        IntStream.range(0, 100).forEachOrdered(n -> {
            pQueue.enqueue(n, n);
        });
        testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
        Assert.assertEquals(pQueue.internalPriorityQueue.size, 100);
        IntStream.range(0, 100).forEachOrdered(n -> {
            try {
                Assert.assertEquals(pQueue.top(), new Pair<>(n, n));
                pQueue.dequeue();

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(pQueue.internalPriorityQueue.size, 0);
    }

    @Test
    public void testTXPriorityQueueSingletonMultiThread() throws InterruptedException {
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
    }
}

class Run implements Runnable {

    private PriorityQueue pQueue;
    private int priorityRef;
    private String threadName;
    private CountDownLatch latch;
    private static final CyclicBarrier barrier = new CyclicBarrier(100);


    Run(String name, CountDownLatch l, PriorityQueue pq, int priorityRef) {
        this.threadName = name;
        this.latch = l;
        this.pQueue = pq;
        this.priorityRef = priorityRef;
    }

    private static void barrierAwaitWrapper() {
        try {
            Run.barrier.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    private void barrierResetWrapper() {
        if (threadName == "T0") {
            Run.barrier.reset();
        }
    }

    @Override
    public void run() {
        try {
            latch.await();
        } catch (InterruptedException exp) {
            //System.out.println(threadName + ": InterruptedException");
        }

        Assert.assertEquals(true, pQueue.isEmpty());
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();

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

        //System.out.println(threadName + ": enqueue(" + p_a + "," + a + ")");
        pQueue.enqueue(p_a, a);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();

        Assert.assertEquals(100, pQueue.internalPriorityQueue.size);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        try {
            //System.out.println(threadName + ": dequeue()");
            pQueue.dequeue();
        } catch (TXLibExceptions.PQueueIsEmptyException ex) {
            ex.printStackTrace();
            assert false;
        }
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        Assert.assertEquals(0, pQueue.internalPriorityQueue.size);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();

        //System.out.println(threadName + ": enqueue(" + p_a + "," + a + ")");
        pQueue.enqueue(p_a, a);
        //System.out.println(threadName + ": enqueue(" + p_b + "," + b + ")");
        pQueue.enqueue(p_b, b);
        //System.out.println(threadName + ": enqueue(" + p_c + "," + c + ")");
        pQueue.enqueue(p_c, c);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        Assert.assertEquals(300, pQueue.internalPriorityQueue.size);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        //System.out.println(threadName + ": enqueue(" + p_d + "," + d + ")");
        pQueue.enqueue(p_d, d);

        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        Assert.assertEquals(400, pQueue.internalPriorityQueue.size);
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();

        if (threadName == "T0") {
            PriorityQueueSingletonTest.testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
        }
        try {
            Assert.assertEquals(10, pQueue.top().getKey());
        } catch (TXLibExceptions.PQueueIsEmptyException ex) {
            ex.printStackTrace();
        }
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
        Assert.assertEquals(false, pQueue.isEmpty());
        Run.barrierAwaitWrapper();
        this.barrierResetWrapper();
    }
}

