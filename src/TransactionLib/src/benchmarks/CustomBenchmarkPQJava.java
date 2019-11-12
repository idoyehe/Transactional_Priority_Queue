package TransactionLib.src.benchmarks;

import java.util.PriorityQueue;

import TransactionLib.src.main.java.PQObject;
import org.junit.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class CustomBenchmarkPQJava {
    final static int TOTAL_ELEMENTS = 16384;
    final int numberOfThreads = 16;

    final int range = CustomBenchmarkPQJava.TOTAL_ELEMENTS / this.numberOfThreads;

    @Test
    public void testLocalPriorityQueueConstructor() throws InterruptedException {
        PriorityQueue<PQObject> pQueue = new PriorityQueue<PQObject>();
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);
        ReentrantLock pqLock = new ReentrantLock();

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i] = new Thread(new RunTransactions("T" + i, barrier, pqLock, pQueue, this.numberOfThreads, this.range));
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
        private ReentrantLock pqLock;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";
        private int chunk = 4;

        RunTransactions(String name, CyclicBarrier barrier, ReentrantLock pqLock, PriorityQueue pq, int numberOfThreads, int range) {
            this.threadName = name;
            this.barrier = barrier;
            this.pqLock = pqLock;
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

            //enqueue episode
            int offset = this.range / this.chunk;
            long start = System.currentTimeMillis();
            for (int j = 0; j < offset; j++) {
                this.pqLock.lock();
                for (int k = 0; k < this.chunk; k++) {
                    double rand = Math.random();
                    pQueue.add(new PQObject(rand, rand));
                }
                this.pqLock.unlock();
            }

            long finish = System.currentTimeMillis();
            System.out.printf("Add episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            this.await();
            this.printBorder();

            assertEquals(CustomBenchmarkPQJava.TOTAL_ELEMENTS, this.pQueue.size());


            //top episode
            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j++) {
                this.pqLock.lock();
                pQueue.peek();
                pQueue.poll();
                double rand = Math.random();
                pQueue.add(new PQObject(rand, rand));
                pQueue.peek();
                pQueue.poll();
                rand = Math.random();
                pQueue.add(new PQObject(rand, rand));
                this.pqLock.unlock();

            }

            finish = System.currentTimeMillis();
            System.out.printf("Top episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            this.await();
            assertEquals(CustomBenchmarkPQJava.TOTAL_ELEMENTS, this.pQueue.size());
            this.printBorder();
            this.await();


            //dequeue episode
            start = System.currentTimeMillis();
            for (int j = 0; j < this.range / 2; j++) {
                this.pqLock.lock();
                pQueue.poll();
                pQueue.poll();
                this.pqLock.unlock();

            }

            finish = System.currentTimeMillis();
            System.out.printf("Dequeue episode, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            this.await();

            assertEquals(0, this.pQueue.size());
            this.printBorder();
            this.await();
        }
    }
}
