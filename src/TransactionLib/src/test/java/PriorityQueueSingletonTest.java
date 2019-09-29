package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertEquals;

public class PriorityQueueSingletonTest {
    private void testHeapInvariantRecursive(PQNode node) {
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
            e.printStackTrace();
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
        testHeapInvariantRecursive(pQueue.root.root);
        Assert.assertEquals(pQueue.root.size, 100);
        IntStream.range(0, 100).forEachOrdered(n -> {
            try {
                Assert.assertEquals(pQueue.top(), new Pair<>(n, n));
                pQueue.dequeue();

            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                assert false;
            }
        });
        Assert.assertEquals(pQueue.root.size, 0);
    }

    @Test
    public void testTXPriorityQueueSingletonMultiThread() throws InterruptedException {


        class Run implements Runnable {

            PriorityQueue pq;
            String threadName;
            CountDownLatch latch;

            Run(String name, CountDownLatch l, PriorityQueue pq) {
                this.threadName = name;
                this.latch = l;
                this.pq = pq;
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
                String b_2 = threadName + "-b_2";
                String c = threadName + "-c";
                Integer k_a = 10 + threadName.charAt(1);
//                System.out.println(threadName + ": " + k_a);
                Integer k_b = 20 + threadName.charAt(1);
//                System.out.println(threadName + ": " + k_b);
                Integer k_c = 30 + threadName.charAt(1);
//                System.out.println(threadName + ": " + k_c);

////			 System.out.println(threadName + ": enqueue(k_a, a)");
                pq.enqueue(k_a, a);
////			 System.out.println(threadName + ": containsKey(k_c)");
                try {
                    assertEquals(new Pair<>(k_a, a), pq.top());
                } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    e.printStackTrace();
                    assert false;
                }
////			 System.out.println(threadName + ": enqueue(k_c, c)");
//                assertEquals(null, pq.enqueue(k_c, c));
////			 System.out.println(threadName + ": containsKey(k_a)");
//                assertEquals(true, pq.containsKey(k_a));
////			 System.out.println(threadName + ": containsKey(k_c)");
//                assertEquals(true, pq.containsKey(k_c));
////			 System.out.println(threadName + ": containsKey(k_b)");
//                assertEquals(false, pq.containsKey(k_b));
////			 System.out.println(threadName + ": enqueue(k_b, b)");
//                assertEquals(null, pq.enqueue(k_b, b));
////			 System.out.println(threadName + ": containsKey(k_b)");
//                assertEquals(true, pq.containsKey(k_b));
////			 System.out.println(threadName + ": containsKey(k_c)");
//                assertEquals(true, pq.containsKey(k_c));
////			 System.out.println(threadName + ": enqueue(k_c, c)");
//                assertEquals(c, pq.enqueue(k_c, c));
////			 System.out.println(threadName + ": remove(k_b)");
//                assertEquals(b, pq.remove(k_b));
////			 System.out.println(threadName + ": remove(k_b)");
//                assertEquals(null, pq.remove(k_b));
////			 System.out.println(threadName + ": remove(k_a)");
//                assertEquals(a, pq.remove(k_a));
////			 System.out.println(threadName + ": remove(k_c)");
//                assertEquals(c, pq.remove(k_c));
////			 System.out.println(threadName + ": remove(k_a)");
//                assertEquals(null, pq.remove(k_a));
////			 System.out.println(threadName + ": remove(k_c)");
//                assertEquals(null, pq.remove(k_c));
////			 System.out.println(threadName + ": containsKey(k_b)");
//                assertEquals(false, pq.containsKey(k_b));
////			 System.out.println(threadName + ": containsKey(k_c)");
//                assertEquals(false, pq.containsKey(k_c));
////			 System.out.println(threadName + ": enqueue(k_a, a)");
//                assertEquals(null, pq.enqueue(k_a, a));
////			 System.out.println(threadName + ": enqueue(k_b, b)");
//                assertEquals(null, pq.enqueue(k_b, b));
////			 System.out.println(threadName + ": containsKey(k_a)");
//                assertEquals(true, pq.containsKey(k_a));
////			 System.out.println(threadName + ": enqueue(k_b, b_2)");
//                assertEquals(b, pq.enqueue(k_b, b_2));
////			 System.out.println(threadName + ": get(k_a)");
//                assertEquals(a, pq.get(k_a));
////			 System.out.println(threadName + ": containsKey(k_b)");
//                assertEquals(true, pq.containsKey(k_b));
////			 System.out.println(threadName + ": get(k_b)");
//                assertEquals(b_2, pq.get(k_b));
////			 System.out.println(threadName + ": remove(-1)");
//                assertEquals(null, pq.remove(-1));
////			 System.out.println(threadName + ": remove(k_b)");
//                assertEquals(b_2, pq.remove(k_b));
////			 System.out.println(threadName + ": remove(k_b)");
//                assertEquals(null, pq.remove(k_b));
////			 System.out.println(threadName + ": enqueue(k_c, c)");
//                assertEquals(null, pq.enqueue(k_c, c));
////			 System.out.println(threadName + ": remove(k_c)");
//                assertEquals(c, pq.remove(k_c));
////			 System.out.println(threadName + ": remove(k_c)");
//                assertEquals(null, pq.remove(k_c));
////			 System.out.println(threadName + ": remove(k_a)");
//                assertEquals(a, pq.remove(k_a));
////			 System.out.println(threadName + ": remove(k_a)");
//                assertEquals(null, pq.remove(k_a));
////			 System.out.println(threadName + ": containsKey(k_a)");
//                assertEquals(false, pq.containsKey(k_a));
////			 System.out.println(threadName + ": enqueue(k_a, a)");
//                assertEquals(null, pq.enqueue(k_a, a));
////			 System.out.println(threadName + ": remove(k_a)");
//                assertEquals(a, pq.remove(k_a));
//
//                assertEquals(null, pq.enqueueIfAbsent(k_a, a));
//                assertEquals(a, pq.enqueueIfAbsent(k_a, ""));
//                assertEquals(null, pq.enqueueIfAbsent(k_c, c));
//                assertEquals(c, pq.enqueueIfAbsent(k_c, ""));
//                assertEquals(null, pq.enqueueIfAbsent(k_b, b));
//                assertEquals(b, pq.enqueueIfAbsent(k_b, ""));
//                assertEquals(c, pq.remove(k_c));
//                assertEquals(a, pq.remove(k_a));
//                assertEquals(b, pq.remove(k_b));
////			 System.out.println(threadName + ": end");
            }
        }
        CountDownLatch latch = new CountDownLatch(1);
        PriorityQueue pQueue = new PriorityQueue();
        Thread T1 = new Thread(new Run("T1", latch, pQueue));
        Thread T2 = new Thread(new Run("T2", latch, pQueue));
        Thread T3 = new Thread(new Run("T3", latch, pQueue));
        T1.start();
        T2.start();
        T3.start();
        latch.countDown();
        T1.join();
        T2.join();
        T3.join();
    }
}
