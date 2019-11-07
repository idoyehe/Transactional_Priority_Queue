package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;
import org.junit.Test;

import static junit.framework.TestCase.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;


public class PriorityQueueMultiThreadsTest {
    final int numberOfThreads = 200;


    @Test
    public void testPriorityQueueMultiThreadSingleton() throws InterruptedException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(true);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i] = new Thread(new RunSingletone("T" + i, barrier, pQueue, i * numberOfThreads, numberOfThreads));
            threadsARR[i].start();
        }

        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i].join();
        }
    }

    class RunSingletone implements Runnable {

        protected PriorityQueue pQueue;
        protected int priorityRef;
        protected int numberOfThread;
        protected String threadName;
        protected CyclicBarrier barrier;
        protected final String masterThread = "T0";

        RunSingletone(String name, CyclicBarrier barrier, PriorityQueue pq, int priorityRef, int numberOgThread) {
            this.threadName = name;
            this.barrier = barrier;
            this.pQueue = pq;
            this.priorityRef = priorityRef;
            this.numberOfThread = numberOgThread;
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
            PQObject globalNodesArr[] = new PQObject[this.numberOfThread];

            IntStream.range(0, this.numberOfThread).map(i -> this.numberOfThread - 1 - i).forEach(n -> {
                final PQObject newNode = pQueue.enqueue(n + this.priorityRef, n + this.priorityRef);
                globalNodesArr[n] = newNode;
                assertEquals(n + this.priorityRef, newNode.getPriority());
            });
            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * this.numberOfThread, pQueue.size());

            if (this.threadName.equals(this.masterThread)) {
                IntStream.range(0, this.numberOfThread).map(i -> this.numberOfThread - 1 - i).forEach(n -> {
                    final PQObject newNode = pQueue.decreasePriority(globalNodesArr[n], -(Integer) globalNodesArr[n].getPriority());
                    assertEquals(newNode, globalNodesArr[n]);
                });
            }
            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * this.numberOfThread, pQueue.size());

            try {
                assertEquals(this.numberOfThread - 1, pQueue.top());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
            }

            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * this.numberOfThread, pQueue.size());
            this.await();

            try {
                assertTrue((Integer) pQueue.dequeue() < this.numberOfThread);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
            }

            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * (this.numberOfThread - 1), pQueue.size());
            this.await();

            if (!this.threadName.equals(this.masterThread)) {
                IntStream.range(0, this.numberOfThread).forEach(n -> {
                    try {
                        assertTrue((Integer) pQueue.dequeue() >= this.numberOfThread);
                    } catch (TXLibExceptions.PQueueIsEmptyException e) {
                        e.printStackTrace();
                    }
                });
            }


            this.await();
            assertTrue(pQueue.isEmpty());
            assertEquals(0, pQueue.size());
            this.await();
        }
    }

    @Test
    public void testPriorityQueueMultiThreadTransaction() throws InterruptedException {
        PriorityQueue pQueue = new PriorityQueue();
        pQueue.setSingleton(false);
        CyclicBarrier barrier = new CyclicBarrier(this.numberOfThreads);

        Thread[] threadsARR = new Thread[this.numberOfThreads];
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i] = new Thread(new RunTransaction("T" + i, barrier, pQueue, i * numberOfThreads, numberOfThreads));
            threadsARR[i].start();
        }
        for (int i = 0; i < this.numberOfThreads; i++) {
            threadsARR[i].join();
        }
    }

    class RunTransaction extends RunSingletone {


        RunTransaction(String name, CyclicBarrier barrier, PriorityQueue pq, int priorityRef, int numberOfThread) {
            super(name, barrier, pq, priorityRef, numberOfThread);
        }

        @Override
        public void run() {
            try {
                this.barrier.await();
            } catch (BrokenBarrierException | InterruptedException exp) {
                System.out.println(threadName + ": InterruptedException");
            }
            PQObject globalNodesArr[] = new PQObject[this.numberOfThread];

            //first transaction
            while (true) {
                try {
                    try {
                        TX.TXbegin();
                        int initSize = pQueue.size();
                        IntStream.range(0, this.numberOfThread).map(i -> this.numberOfThread - 1 - i).forEach(n -> {
                            final PQObject newNode = pQueue.enqueue(n + this.priorityRef, n + this.priorityRef);
                            globalNodesArr[n] = newNode;
                            assertEquals(n + this.priorityRef, newNode.getPriority());
                        });
                        assertFalse(pQueue.isEmpty());
                        assertEquals(initSize + numberOfThread, pQueue.size());
                    } finally {
                        TX.TXend();
                    }
                } catch (TXLibExceptions.AbortException exp) {
                    continue;
                }
                break;
            }
            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * this.numberOfThread, pQueue.size());
            pQueue.setSingleton(false);
            this.await();

            PQObject localNodesArr[] = new PQObject[this.numberOfThread];

            while (true) {
                try {
                    try {
                        TX.TXbegin();

                        IntStream.range(0, this.numberOfThread).forEach(n -> {
                            localNodesArr[n] = pQueue.decreasePriority(globalNodesArr[n], -(Integer) globalNodesArr[n].getPriority() - 1);
                            assertTrue(globalNodesArr[n] != localNodesArr[n]);
                        });
                        try {
                            assertTrue((Integer) pQueue.dequeue() >= this.priorityRef);
                        } catch (TXLibExceptions.PQueueIsEmptyException e) {
                            fail("PriorityQueue should not be empty");
                        }
                    } finally {
                        TX.TXend();
                    }
                } catch (TXLibExceptions.AbortException exp) {
                    continue;
                }
                break;
            }
            IntStream.range(0, this.numberOfThread).forEach(n -> {
                globalNodesArr[n] = localNodesArr[n];
                localNodesArr[n] = null;
            });

            this.await();
            assertFalse(pQueue.isEmpty());
            assertEquals(this.numberOfThread * (this.numberOfThread - 1), pQueue.size());
            pQueue.setSingleton(false);
            this.await();

            while (true) {
                try {
                    try {
                        TX.TXbegin();
                        int initSize = pQueue.size();
                        IntStream.range(1, this.numberOfThread).forEach(n -> {
                            try {
                                pQueue.dequeue();
                            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                                fail("PriorityQueue should not be empty");
                            }
                        });
                        assertEquals(initSize - this.numberOfThread + 1, pQueue.size());
                    } finally {
                        TX.TXend();
                    }
                } catch (TXLibExceptions.AbortException exp) {
                    continue;
                }
                break;
            }
            this.await();
            assertTrue(pQueue.isEmpty());
            assertEquals(0, pQueue.size());
        }
    }
}

