package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import javafx.util.Pair;
import org.junit.Test;
import java.util.stream.IntStream;

import static junit.framework.TestCase.*;

public class PriorityQueueTXTest {
    private void testHeapInvariantRecursive(PQNode node) {
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
        testHeapInvariantRecursive(pQueue.internalPriorityQueue.root);
    }

    @Test
    public void testPriorityQueueSingleThreadDequeueLocalTXStorage() throws TXLibExceptions.PQueueIsEmptyException {
        PriorityQueue pQueue = new PriorityQueue();

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

//    @Test
//    public void testLinkedListMultiThread() throws InterruptedException {
//        CountDownLatch latch = new CountDownLatch(1);
//        LinkedList LL = new LinkedList();
//        Thread T1 = new Thread(new Run("T1", latch, LL));
//        Thread T2 = new Thread(new Run("T2", latch, LL));
//        Thread T3 = new Thread(new Run("T3", latch, LL));
//        T1.start();
//        T2.start();
//        T3.start();
//        latch.countDown();
//        T1.join();
//        T2.join();
//        T3.join();
//    }
}
//
//    class Run implements Runnable {
//
//        LinkedList LL;
//        String threadName;
//        CountDownLatch latch;
//
//        Run(String name, CountDownLatch l, LinkedList ll) {
//            threadName = name;
//            latch = l;
//            LL = ll;
//        }
//
//        @Override
//        public void run() {
//            try {
//                latch.await();
//            } catch (InterruptedException exp) {
//                System.out.println(threadName + ": InterruptedException");
//            }
//            String a = threadName + "-a";
//            String b = threadName + "-b";
//            String c = threadName + "-c";
//            String empty = "";
//            Integer k_a = 10 + threadName.charAt(1);
//            Integer k_b = 20 + threadName.charAt(1);
//            Integer k_c = 30 + threadName.charAt(1);
//
//            while (true) {
//                try {
//                    try {
//                        TX.TXbegin();
//                        assertEquals(false, LL.containsKey(k_c));
//                        assertEquals(null, LL.get(k_c));
//                        assertEquals(null, LL.put(k_c, c));
//                        assertEquals(true, LL.containsKey(k_c));
//                        assertEquals(false, LL.containsKey(k_a));
//                        assertEquals(false, LL.containsKey(k_b));
//                        assertEquals(null, LL.get(k_b));
//                        assertEquals(null, LL.put(k_a, a));
//                        assertEquals(null, LL.put(k_b, b));
//                        assertEquals(true, LL.containsKey(k_b));
//                        assertEquals(true, LL.containsKey(k_a));
//                        assertEquals(a, LL.put(k_a, a));
//                        assertEquals(b, LL.put(k_b, b));
//                        assertEquals(c, LL.get(k_c));
//                        assertEquals(a, LL.get(k_a));
//                        assertEquals(b, LL.get(k_b));
//                        assertEquals(null, LL.remove(-1));
//                        assertEquals(b, LL.remove(k_b));
//                        assertEquals(null, LL.remove(k_b));
//                        assertEquals(false, LL.containsKey(k_b));
//                        assertEquals(a, LL.remove(k_a));
//                        assertEquals(c, LL.remove(k_c));
//                        assertEquals(null, LL.remove(k_a));
//                        assertEquals(null, LL.get(k_c));
//                        assertEquals(null, LL.put(k_b, b));
//                        assertEquals(b, LL.get(k_b));
//                        assertEquals(b, LL.put(k_b, empty));
//                        assertEquals(empty, LL.get(k_b));
//                        assertEquals(null, LL.put(k_c, c));
//                        assertEquals(c, LL.get(k_c));
//                        assertEquals(c, LL.put(k_c, empty));
//                        assertEquals(empty, LL.get(k_c));
//                        assertEquals(empty, LL.remove(k_b));
//                        assertEquals(empty, LL.remove(k_c));
//                        assertEquals(null, LL.putIfAbsent(k_c, c));
//                        assertEquals(c, LL.putIfAbsent(k_c, empty));
//                        assertEquals(c, LL.putIfAbsent(k_c, empty));
//                        assertEquals(c, LL.get(k_c));
//                        assertEquals(null, LL.putIfAbsent(k_b, b));
//                        assertEquals(b, LL.putIfAbsent(k_b, empty));
//                        assertEquals(b, LL.putIfAbsent(k_b, empty));
//                        assertEquals(b, LL.get(k_b));
//                        assertEquals(c, LL.remove(k_c));
//                        assertEquals(null, LL.putIfAbsent(k_c, c));
//                    } finally {
//                        TX.TXend();
//                    }
//                } catch (TXLibExceptions.AbortException exp) {
//                    continue;
//                }
//                break;
//            }
//
//        }
//    }
//
//}
