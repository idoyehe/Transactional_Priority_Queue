package TransactionLib.src.benchmarks;


import TransactionLib.src.main.java.PQObject;

import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;


public class ConvertedBenchmarkJAVAPQ {
    public static void main(String[] args) throws InterruptedException {
        ConvertedBenchmarkJAVAPQ convertedBenchmark = new ConvertedBenchmarkJAVAPQ();
        convertedBenchmark.runConvertedBenchmark();
    }

    private final int numberOfThreads = 2;
    private final boolean IS_EXP = true;

    private final static int EXPS_POW = 24;
    private final static int EXPS = (int) Math.pow(2, EXPS_POW);

    private final static int INIT_SIZE_POW = 20;
    private final int INIT_SIZE = (int) Math.pow(2, INIT_SIZE_POW);

    private final int TOTAL_WORKLOAD_ELEMENTS = EXPS - INIT_SIZE;
    private final int TOTAL_WORKLOAD_PER_THREAD = TOTAL_WORKLOAD_ELEMENTS / numberOfThreads;

    private final boolean UNIFORM_SINGLETON = true;

    private long[] exps;
    private PriorityBlockingQueue pQueue;
    private AtomicInteger exps_pos = new AtomicInteger(); //This is the VC of the queue

    ConvertedBenchmarkJAVAPQ() {
        this.exps = new long[this.EXPS];
        this.pQueue = new PriorityBlockingQueue();
        this.initPrioritiesQueues();
    }


    private void initPrioritiesQueues() {
        if (this.IS_EXP) {
            genExps(exps);
        }
        for (int i = 0; i < this.INIT_SIZE; i++) {
            if (this.IS_EXP) {
                long elem = exps[this.exps_pos.getAndIncrement()];
                pQueue.add(new PQObject(elem, elem));
            } else {
                long generatedLong = new Random().nextLong();
                pQueue.add(new PQObject(generatedLong, generatedLong));
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
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);
        ReentrantLock pqLock = new ReentrantLock();

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        long start = System.currentTimeMillis();
        if (this.IS_EXP) {
            PriorityQueue<PQObject> regularPQ = new PriorityQueue<>();
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i] = new Thread(new RunDESBenchmark("T" + i, barrier, pqLock, regularPQ, this.exps, this.TOTAL_WORKLOAD_PER_THREAD, this.INIT_SIZE + this.TOTAL_WORKLOAD_PER_THREAD * i));
                threadsARR[i].start();
            }
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i].join();
            }
            long finish = System.currentTimeMillis();
            System.out.printf("DES benchmark, elapsed time: %d [ms]%n", finish - start);
            System.out.printf("DES benchmark, Priority Queue size %d%n", pQueue.size());
        } else {
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i] = new Thread(new RunUniformBenchmarkSingleton("T" + i, barrier, this.pQueue));
                threadsARR[i].start();
            }
            for (int i = 0; i < this.numberOfThreads; i++) {
                threadsARR[i].join();
            }
            long finish = System.currentTimeMillis();
            System.out.printf("Uniform benchmark, elapsed time: %d [ms]%n", finish - start);
            System.out.printf("Uniform benchmark, Priority Queue size %d%n", pQueue.size());
        }
    }
}

class RunUniformBenchmarkSingleton implements Runnable {
    private PriorityBlockingQueue pQueue;
    private String threadName;
    private CyclicBarrier barrier;
    private final String masterThread = "T0";

    RunUniformBenchmarkSingleton(String name, CyclicBarrier barrier, PriorityBlockingQueue pq) {
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
        if (Math.random() < 0.5) {
            long generatedLong = new Random().nextLong();
            this.pQueue.add(new PQObject(generatedLong, generatedLong));
        } else {
            this.pQueue.poll();
        }
        uniformOperationsCounter++;
        long finish = System.currentTimeMillis();
        System.out.printf("Uniform benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
        System.out.printf("Uniform benchmark, Thread name %s, operations counts: %d%n", this.threadName, uniformOperationsCounter);
    }
}

class RunDESBenchmark implements Runnable {
    private PriorityQueue<PQObject> pQueue;
    private String threadName;
    private CyclicBarrier barrier;
    private final String masterThread = "T0";
    private long[] exps;
    private int total_work;
    private int initPOS;
    private ReentrantLock pqLock;


    RunDESBenchmark(String name, CyclicBarrier barrier, ReentrantLock pqLock, PriorityQueue pq, long[] exps, int total_work, int initPOS) {
        this.threadName = name;
        this.barrier = barrier;
        this.pQueue = pq;
        this.exps = exps;
        this.initPOS = initPOS;
        this.total_work = total_work;
        this.pqLock = pqLock;
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
        int pos = this.initPOS;

        int desOperationsCounter = 0;
        for (int i = 0; i < this.total_work; i++) {
            this.pqLock.lock();
            this.pQueue.poll();
            long elem = exps[pos++];
            this.pQueue.add(new PQObject(elem, elem));
            this.pqLock.unlock();
            desOperationsCounter += 2;
        }


        long finish = System.currentTimeMillis();
        System.out.printf("DES benchmark, Thread name %s, elapsed time: %d [ms]%n", this.threadName, finish - start);
        System.out.printf("DES benchmark, Thread name %s, operations counts: %d%n", this.threadName, desOperationsCounter);
    }
}

