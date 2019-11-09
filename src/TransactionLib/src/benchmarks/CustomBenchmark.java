package TransactionLib.src.benchmarks;

import TransactionLib.src.main.java.*;
import org.junit.Test;

import static junit.framework.TestCase.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CustomBenchmark {
    final static int TOTAL_ELEMENTS = 16384;
    final int numberOfThreads = 16;

//    final static int TOTAL_ELEMENTS = 4;
//    final int numberOfThreads = 1;

    final int range = CustomBenchmark.TOTAL_ELEMENTS / this.numberOfThreads;

    @Test
    public void testLocalPriorityQueueConstructor() throws InterruptedException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i] = new Thread(new RunTransactions("T" + i, barrier, pQueue, this.numberOfThreads, this.range));
            threadsARR[i].start();
        }
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i].join();
        }
    }

    class RunTransactions implements Runnable {
        private PriorityQueue pQueue;
        private int numberOfThreads;
        private int range;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";
        private int chunk = 4;

        RunTransactions(String name, CyclicBarrier barrier, PriorityQueue pq, int numberOfThreads, int range) {
            this.threadName = name;
            this.barrier = barrier;
            this.pQueue = pq;
            this.numberOfThreads = numberOfThreads;
            this.range = range;
            this.chunk = 4;
        }

        private void printBorder() {
            if (this.threadName.equals(masterThread)) {
                System.out.printf("%n=========================================================================================================================================================%n%n");
            }
        }

        protected void await() {
            try {
                this.barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }

        @Override
        public void run() {
            try {
                this.barrier.await();
            } catch (BrokenBarrierException | InterruptedException exp) {
                System.out.println(threadName + ": InterruptedException");
            }
            PQObject globalNodesArr[] = new PQObject[this.range];

            //first transaction
            pQueue.setSingleton(false);
            int offset = this.range / this.chunk;
            long start = System.currentTimeMillis();
            int abortCount = 0;
            for (int j = 0; j < offset; j++) {
                while (true) {
                    try {
                        try {
                            TX.TXbegin();
                            for (int k = 0; k < this.chunk; k++) {
                                double rand = Math.random();
                                globalNodesArr[(j * this.chunk) + k] = pQueue.enqueue(rand, rand);
                            }
                        } finally {
                            TX.TXend();
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        abortCount++;
                        continue;
                    }
                    break;
                }
            }
            long finish = System.currentTimeMillis();
            System.out.printf("First episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("First episode, Thread name %s, abort counts: %d%n", this.threadName, abortCount);
            this.await();
            this.printBorder();
            assertEquals(CustomBenchmark.TOTAL_ELEMENTS, this.pQueue.size());
            pQueue.setSingleton(true);

//            while (!pQueue.isEmpty()) {
//                try {
//                    System.out.println(pQueue.top());
//                    pQueue.dequeue();
//                } catch (TXLibExceptions.PQueueIsEmptyException e) {
//                    e.printStackTrace();
//                }
//            }

            //second transaction
            pQueue.setSingleton(false);

            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j++) {
                while (true) {
                    try {
                        TX.TXbegin();
                        try {
                            double rand = Math.random();
                            pQueue.dequeue();
                            pQueue.enqueue(rand, rand);
                            pQueue.decreasePriority(globalNodesArr[j], (double) globalNodesArr[0].getPriority() - rand);

                        } catch (TXLibExceptions.PQueueIsEmptyException e) {
                            assert false;
                        } finally {
                            TX.TXend();
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        abortCount++;
                        continue;
                    }
                    break;
                }
            }
            finish = System.currentTimeMillis();
            System.out.printf("Second episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Second episode, Thread name %s, abort counts: %d%n", this.threadName, abortCount);
            this.await();
            assertEquals(CustomBenchmark.TOTAL_ELEMENTS, this.pQueue.size());
            this.printBorder();
            this.await();
        }
    }
}
