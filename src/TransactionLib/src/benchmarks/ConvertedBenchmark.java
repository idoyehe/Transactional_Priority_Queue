package TransactionLib.src.benchmarks;

import TransactionLib.src.main.java.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.fail;


public class ConvertedBenchmark {
    final int numberOfThreads = 1;

    final boolean IS_EXP = true;
    final int EXPS = 10000;
    final int INIT_SIZE = 10;
    private long[] exps;
    PriorityQueue pQueue;
    private AtomicInteger exps_pos = new AtomicInteger(); //This is the VC of the queue

    @Before
    public void setUp() throws Exception {
        this.exps = new long[this.EXPS];
        this.pQueue = new PriorityQueue();
        this.initPrioritiesQueues();
    }


    private void initPrioritiesQueues() {
        if (this.IS_EXP) {
            genExps(exps);
        }
        for (int i = 0; i < this.INIT_SIZE; i++) {
            if (this.IS_EXP) {
                long elem = exps[this.exps_pos.getAndIncrement()];
                pQueue.enqueue(elem, elem);
            } else {
                long generatedLong = new Random().nextLong();
                pQueue.enqueue(generatedLong, generatedLong);
            }
        }
    }

    /* generate array of exponentially distributed variables */
    private static void genExps(long[] arr) {
        int i = 0;
        arr[0] = 2;
        while (++i < arr.length)
            arr[i] = arr[i - 1] + nextGeometric(2);
    }    /* generate array of exponentially distributed variables */

    private static long nextGeometric(double geoSeed) {
        double p = 1.0 / ((double) geoSeed);
        return (long) (Math.ceil(Math.log(new Random().nextDouble()) / Math.log(1.0 - p)));
    }

    @Test
    public void testConvertedBenchmark() throws InterruptedException {
        this.pQueue.setSingleton(false);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        long start = System.currentTimeMillis();
        if (this.IS_EXP) {
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i] = new Thread(new RunDESBenchmark("T" + i, barrier, this.pQueue, this.exps_pos, this.exps));
                threadsARR[i].start();
            }
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i].join();
            }
            long finish = System.currentTimeMillis();
            pQueue.setSingleton(true);
            System.out.printf("DES benchmark, elapsed time: %d [ms]%n", finish - start);
            System.out.printf("DES benchmark, Priority Queue size %d%n", pQueue.size());
        } else {
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i] = new Thread(new RunUniformBenchmark("T" + i, barrier, this.pQueue));
                threadsARR[i].start();
            }
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i].join();
            }
            long finish = System.currentTimeMillis();
            pQueue.setSingleton(true);
            System.out.printf("Uniform benchmark, elapsed time: %d [ms]%n", finish - start);
            System.out.printf("Uniform benchmark, Priority Queue size %d%n", pQueue.size());
        }
    }

    class RunUniformBenchmark implements Runnable {
        private PriorityQueue pQueue;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";

        RunUniformBenchmark(String name, CyclicBarrier barrier, PriorityQueue pq) {
            this.threadName = name;
            this.barrier = barrier;
            this.pQueue = pq;
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
            this.await();
            long start = System.currentTimeMillis();
            int uniformCounter = 0;
            while (true) {
                try {
                    TX.TXbegin();
                    try {
                        if (Math.random() < 0.5) {
                            long generatedLong = new Random().nextLong();
                            this.pQueue.enqueue(generatedLong, generatedLong);
                        } else {
                            this.pQueue.dequeue();
                        }
                    } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    } finally {
                        TX.TXend();
                    }
                } catch (TXLibExceptions.AbortException e) {
                    uniformCounter++;
                    continue;
                }
                break;
            }
            long finish = System.currentTimeMillis();
            System.out.printf("Uniform benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Uniform benchmark, Thread name %s, abort counts: %d%n", this.threadName, uniformCounter);
        }
    }

    class RunDESBenchmark implements Runnable {
        private PriorityQueue pQueue;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";
        private AtomicInteger exps_pos;
        private long[] exps;


        RunDESBenchmark(String name, CyclicBarrier barrier, PriorityQueue pq, AtomicInteger exps_pos, long[] exps) {
            this.threadName = name;
            this.barrier = barrier;
            this.pQueue = pq;
            this.exps_pos = exps_pos;
            this.exps = exps;
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
            this.await();
            long start = System.currentTimeMillis();
            int uniformCounter = 0;
            int pos = this.exps_pos.getAndIncrement();
            while (pos < exps.length) {
                try {
                    TX.TXbegin();
                    try {
                        this.pQueue.dequeue();
                        long elem = exps[pos];
                        this.pQueue.enqueue(elem, elem);
                    } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    } finally {
                        TX.TXend();
                        pos = this.exps_pos.getAndIncrement();
                    }
                } catch (TXLibExceptions.AbortException e) {
                    uniformCounter++;
                    continue;
                }
            }
            long finish = System.currentTimeMillis();
            System.out.printf("DES benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("DES benchmark, Thread name %s, abort counts: %d%n", this.threadName, uniformCounter);
        }
    }
}
