package TransactionLib.src.benchmarks;

import TransactionLib.src.main.java.*;
import org.junit.Test;

import static junit.framework.TestCase.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CustomBenchmark {
    final static int TOTAL_ELEMENTS = 16384;
    final int numberOfThreads = 16;

    final int range = CustomBenchmark.TOTAL_ELEMENTS / this.numberOfThreads;

    @Test
    public void testCustomBenchmark() throws InterruptedException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        for (int i = 0; i < CustomBenchmark.TOTAL_ELEMENTS*5; i++) {
            double rand = Math.random();
            pQueue.enqueue(rand, rand);
        }
        pQueue.setSingleton(false);

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
            PQNode globalNodesArr[] = new PQNode[this.range];

            //enqueue episode
            pQueue.setSingleton(false);
            int offset = this.range / this.chunk;
            long start = System.currentTimeMillis();
            int enqueueAbortCount = 0;
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
                        enqueueAbortCount++;
                        continue;
                    }
                    break;
                }
            }
            long finish = System.currentTimeMillis();
            System.out.printf("Enqueue episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Enqueue episode, Thread name %s, abort counts: %d%n", this.threadName, enqueueAbortCount);
            this.await();
            this.printBorder();
//            assertEquals(CustomBenchmark.TOTAL_ELEMENTS * 3, this.pQueue.size());
            pQueue.setSingleton(true);

            //decreasePriority episode
            pQueue.setSingleton(false);

            int decreasePriorityAbortCounter = 0;
            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j += 2) {
                while (true) {
                    try {
                        TX.TXbegin();
                        try {
                            double rand = Math.random();
                            globalNodesArr[j] = pQueue.decreasePriority(globalNodesArr[j], (double) globalNodesArr[j].getPriority() - rand);
                            globalNodesArr[j + 1] = pQueue.decreasePriority(globalNodesArr[j + 1], (double) globalNodesArr[j + 1].getPriority() - rand);
                        } finally {
                            TX.TXend();
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        decreasePriorityAbortCounter++;
                        continue;
                    }
                    break;
                }
            }
            finish = System.currentTimeMillis();
            System.out.printf("Decrease Priority episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Decrease Priority episode, Thread name %s, abort counts: %d%n", this.threadName, decreasePriorityAbortCounter);
            this.await();
//            assertEquals(CustomBenchmark.TOTAL_ELEMENTS * 3, this.pQueue.size());
            this.printBorder();
            this.await();


            //top episode
            pQueue.setSingleton(false);
            int topAbortCounter = 0;
            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j++) {
                while (true) {
                    try {
                        TX.TXbegin();
                        try {
                            pQueue.top();
                            pQueue.dequeue();
                            double rand = Math.random();
                            pQueue.enqueue(rand, rand);
                            pQueue.top();
                            pQueue.dequeue();
                            rand = Math.random();
                            pQueue.enqueue(rand, rand);
                        } catch (TXLibExceptions.PQueueIsEmptyException e) {
                            assert false;
                        } finally {
                            TX.TXend();
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        topAbortCounter++;
                        continue;
                    }
                    break;
                }
            }
            finish = System.currentTimeMillis();
            System.out.printf("Top episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Top episode, Thread name %s, abort counts: %d%n", this.threadName, topAbortCounter);
            this.await();
//            assertEquals(CustomBenchmark.TOTAL_ELEMENTS * 3, this.pQueue.size());
            this.printBorder();
            this.await();            //top episode
            pQueue.setSingleton(false);


            //dequeue episode
            pQueue.setSingleton(false);
            int dequeueAbortCounter = 0;
            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j++) {
                while (true) {
                    try {
                        TX.TXbegin();
                        try {
                            pQueue.dequeue();
                            pQueue.dequeue();
                        } catch (TXLibExceptions.PQueueIsEmptyException e) {
                            assert false;
                        } finally {
                            TX.TXend();
                        }
                    } catch (TXLibExceptions.AbortException exp) {
                        dequeueAbortCounter++;
                        continue;
                    }
                    break;
                }
            }
            finish = System.currentTimeMillis();
            System.out.printf("Dequeue episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Dequeue episode, Thread name %s, abort counts: %d%n", this.threadName, dequeueAbortCounter);
            this.await();
//            assertEquals(CustomBenchmark.TOTAL_ELEMENTS*2, this.pQueue.size());
            this.printBorder();
            this.await();
        }
    }
}
