package TransactionLib.src.benchmarks;

import TransactionLib.src.main.java.*;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;


public class ConvertedBenchmark {
    public static void main(String[] args) throws InterruptedException {
        ConvertedBenchmark convertedBenchmark = new ConvertedBenchmark();
        convertedBenchmark.runConvertedBenchmark();
    }

    private final int numberOfThreads = 32;
    private final boolean IS_EXP = true;

    private final static int EXPS_POW = 20;
    private final static int EXPS = (int) Math.pow(2, EXPS_POW);

    private final static int INIT_SIZE_POW = 12;
    private final int INIT_SIZE = (int) Math.pow(2, INIT_SIZE_POW);

    private final boolean UNIFORM_SINGLETON = true;

    private final int TOTAL_WORKLOAD_ELEMENTS = EXPS - INIT_SIZE;
    private final int TOTAL_WORKLOAD_PER_THREAD = TOTAL_WORKLOAD_ELEMENTS / numberOfThreads;

    private long[] exps;
    private PriorityQueue pQueue;
    private AtomicInteger exps_pos = new AtomicInteger(); //This is the VC of the queue

    ConvertedBenchmark() {
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

    public void runConvertedBenchmark() throws InterruptedException {
        this.pQueue.setSingleton(false);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        long start = System.currentTimeMillis();
        if (this.IS_EXP) {
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i] = new Thread(new RunDESBenchmark("T" + i, barrier, this.pQueue, this.exps, this.TOTAL_WORKLOAD_PER_THREAD, this.INIT_SIZE + this.TOTAL_WORKLOAD_PER_THREAD * i));
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
            if (!this.UNIFORM_SINGLETON) {
                for (int i = 0; i < this.numberOfThreads; i++) {
                    threadsARR[i] = new Thread(new RunUniformBenchmarkTransactions("T" + i, barrier, this.pQueue));
                    threadsARR[i].start();
                }
                for (int i = 0; i < this.numberOfThreads; i++) {
                    threadsARR[i].join();
                }
                long finish = System.currentTimeMillis();
                pQueue.setSingleton(true);
                System.out.printf("Uniform benchmark, elapsed time: %d [ms]%n", finish - start);
                System.out.printf("Uniform benchmark, Priority Queue size %d%n", pQueue.size());
            } else {
                this.pQueue.setSingleton(true);
                for (int i = 0; i < this.numberOfThreads; i++) {
                    threadsARR[i] = new Thread(new RunUniformBenchmarkSingleton("T" + i, barrier, this.pQueue));
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
    }

    class RunUniformBenchmarkTransactions implements Runnable {
        private PriorityQueue pQueue;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";

        RunUniformBenchmarkTransactions(String name, CyclicBarrier barrier, PriorityQueue pq) {
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
            }
        }

        @Override
        public void run() {
            this.await();
            long start = System.currentTimeMillis();
            int uniformCounter = 0;
            int uniformOperationsCounter = 0;
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
                        uniformOperationsCounter++;
                    }
                } catch (TXLibExceptions.AbortException e) {
                    uniformCounter++;
                    continue;
                }
                break;
            }
            long finish = System.currentTimeMillis();
            System.out.printf("Uniform benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Uniform benchmark, Thread name %s, operations counts: %d%n", this.threadName, uniformOperationsCounter);
            System.out.printf("Uniform benchmark, Thread name %s, abort counts: %d%n", this.threadName, uniformCounter);
        }
    }

    class RunUniformBenchmarkSingleton implements Runnable {
        private PriorityQueue pQueue;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";

        RunUniformBenchmarkSingleton(String name, CyclicBarrier barrier, PriorityQueue pq) {
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
            }
        }

        @Override
        public void run() {
            this.await();
            long start = System.currentTimeMillis();
            int uniformOperationsCounter = 0;
            try {
                if (Math.random() < 0.5) {
                    long generatedLong = new Random().nextLong();
                    this.pQueue.enqueue(generatedLong, generatedLong);
                } else {
                    this.pQueue.dequeue();
                }
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
            } finally {
                uniformOperationsCounter++;
            }

            long finish = System.currentTimeMillis();
            System.out.printf("Uniform singleton benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("Uniform singleton benchmark, Thread name %s, operations counts: %d%n", this.threadName, uniformOperationsCounter);
        }
    }

    class RunDESBenchmark implements Runnable {
        private PriorityQueue pQueue;
        private String threadName;
        private CyclicBarrier barrier;
        private final String masterThread = "T0";
        private long[] exps;
        private int total_work;
        private int initPOS;


        RunDESBenchmark(String name, CyclicBarrier barrier, PriorityQueue pq, long[] exps, int total_work, int initPOS) {
            this.threadName = name;
            this.barrier = barrier;
            this.pQueue = pq;
            this.exps = exps;
            this.initPOS = initPOS;
            this.total_work = total_work;
        }

        protected void await() {
            try {
                this.barrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            this.await();
            long start = System.currentTimeMillis();
            int uniformCounter = 0;
            int pos = this.initPOS;
            int currentWork = 0;
            int desOperationsCounter = 0;
            while (currentWork < this.total_work) {
                try {
                    TX.TXbegin();
                    try {
                        this.pQueue.dequeue();
                        long elem = exps[pos];
                        this.pQueue.enqueue(elem, elem);
                    } catch (TXLibExceptions.PQueueIsEmptyException e) {
                    } finally {
                        TX.TXend();
                        pos++;
                        currentWork++;
                        desOperationsCounter += 2;
                    }
                } catch (TXLibExceptions.AbortException e) {
                    uniformCounter++;
                    continue;
                }
            }
            long finish = System.currentTimeMillis();
            System.out.printf("DES benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
            System.out.printf("DES benchmark, Thread name %s, operations counts: %d%n", this.threadName, desOperationsCounter);
            System.out.printf("DES benchmark, Thread name %s, abort counts: %d%n", this.threadName, uniformCounter);
        }
    }
}
