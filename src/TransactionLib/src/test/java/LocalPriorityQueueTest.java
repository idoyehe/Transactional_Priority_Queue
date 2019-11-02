package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import org.junit.Test;

import static junit.framework.TestCase.*;
import static junit.framework.TestCase.fail;

import javafx.util.Pair;

import java.util.ArrayList;
import java.util.stream.IntStream;


public class LocalPriorityQueueTest {
    final int range = 5000;

    @Test
    public void testLocalPriorityQueueConstructor() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTopWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.top();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testDequeueWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.dequeue();
    }

    @Test
    public void testSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(1, 1);
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.enqueue(element.getKey(), element.getValue());
        assertEquals(element, lpq.top());
        lpq.testHeapInvariantRecursive();
        assertEquals(1, lpq.size());
    }

    @Test
    public void testSingleEnqueueAndSingleDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        final Pair<Comparable, Object> element = new Pair<>(1, 1);
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.enqueue(element.getKey(), element.getValue());
        assertEquals(element, lpq.top());
        lpq.testHeapInvariantRecursive();
        assertEquals(1, lpq.size());

        assertEquals(element, lpq.dequeue());
        lpq.testHeapInvariantRecursive();
    }

    @Test
    public void testMultiplyEnqueueAndMultiplyDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = lpq.enqueue(n, n);
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(1, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, lpq.size());

        IntStream.range(0, this.range).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(n, n);
                assertEquals(element, lpq.top());
                assertEquals(element, lpq.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void testDecreasePriority() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        PQNode nodesArr[] = new PQNode[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = lpq.enqueue(n, n);
            nodesArr[n] = newRoot;
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(1, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, lpq.size());

        for (int i = 0; i < nodesArr.length; i++) {
            assertEquals(i, nodesArr[i].getPriority());
            assertEquals(i, nodesArr[i].getValue());
        }

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            Integer oldPrio = (Integer) nodesArr[n].getPriority();
            lpq.decreasePriority(nodesArr[n], -oldPrio);
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(-oldPrio, nodesArr[n].getPriority());
        });

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                final Pair<Comparable, Object> element = new Pair<>(-n, n);
                assertEquals(element, lpq.top());
                assertEquals(new Pair<>(nodesArr[n].getPriority(), nodesArr[n].getValue()), lpq.top());
                assertEquals(element, lpq.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        });
    }

    @Test
    public void testSmallestSimulation() {
        PrimitivePriorityQueue anotherPrimitive = new PrimitivePriorityQueue();

        PQNode nodesArr[] = new PQNode[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = anotherPrimitive.enqueue(n, n);
            nodesArr[n] = newRoot;
            try {
                anotherPrimitive.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(1, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });

        assertEquals(this.range, anotherPrimitive.size());

        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).forEachOrdered(n -> {
            try {
                assertEquals(new Pair<>(n, n), lpq.currentSmallest(anotherPrimitive));
                lpq.nextSmallest(anotherPrimitive);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
        });
        assertEquals(0, lpq.getModifiedNodesState().size());

        //decreasing the root
        final LocalPriorityQueue lpq2 = new LocalPriorityQueue();

        try {
            int newPrio = -1;
            assertEquals(new Pair<>(nodesArr[0].getPriority(), nodesArr[0].getValue()), anotherPrimitive.top());
            anotherPrimitive.decreasePriority(nodesArr[0], newPrio);
            assertEquals(new Pair<>(newPrio, nodesArr[0].getValue()), anotherPrimitive.top());
            assertEquals(newPrio, nodesArr[0].getPriority());
            lpq2.addModifiedNode(nodesArr[0]);

            assertEquals(new Pair<>(nodesArr[1].getPriority(), nodesArr[1].getValue()), lpq2.currentSmallest(anotherPrimitive));

            int newPrio2 = 1;
            anotherPrimitive.decreasePriority(nodesArr[1], newPrio2);
            assertEquals(new Pair<>(newPrio, nodesArr[0].getValue()), anotherPrimitive.top());
            assertEquals(newPrio2, nodesArr[1].getPriority());
            lpq2.addModifiedNode(nodesArr[1]);

            assertEquals(new Pair<>(nodesArr[2].getPriority(), nodesArr[2].getValue()), lpq2.currentSmallest(anotherPrimitive));


        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");
        }

        final LocalPriorityQueue lpq3 = new LocalPriorityQueue();
        try {
            anotherPrimitive.dequeue();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");

        }
        nodesArr[0] = anotherPrimitive.enqueue(0, 0);
        try {
            assertEquals(new Pair<>(nodesArr[0].getPriority(), nodesArr[0].getValue()), anotherPrimitive.top());
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");
        }

        for (int i = nodesArr.length / 2; i < nodesArr.length; i++) {
            anotherPrimitive.decreasePriority(nodesArr[i], -(Integer) nodesArr[i].getPriority());
            lpq3.addModifiedNode(nodesArr[i]);
        }
        for (int i = 0; i < nodesArr.length / 2; i++) {
            try {
                assertEquals(new Pair<>(nodesArr[nodesArr.length - 1].getPriority(), nodesArr[nodesArr.length - 1].getValue()), anotherPrimitive.top());
                assertEquals(new Pair<>(i, i), lpq3.currentSmallest(anotherPrimitive));
                lpq3.nextSmallest(anotherPrimitive);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
        }
    }

    @Test
    public void testComprehensiveScenario() {
        PrimitivePriorityQueue primitivePQ = new PrimitivePriorityQueue();
        PQNode nodesArray[] = new PQNode[this.range];

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = primitivePQ.enqueue(n, n);
            nodesArray[n] = newRoot;

            try {
                assertEquals(new Pair<>(n, n), primitivePQ.top());
                assertEquals(1, newRoot.getIndex());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }

            try {
                primitivePQ.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(1, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });

        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            primitivePQ.decreasePriority(nodesArray[n], -n);
            lpq.addModifiedNode(nodesArray[n]);

            try {
                primitivePQ.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(-n, nodesArray[n].getPriority());
        });
        assertEquals(this.range, lpq.getModifiedNodesState().size());


        try {
            lpq.currentSmallest(primitivePQ);
            fail("should throw empty Queue Exception");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
        }
        assertEquals(0, lpq.getModifiedNodesState().size());
        assertEquals(this.range, lpq.dequeueCounter());

        final LocalPriorityQueue lpq2 = new LocalPriorityQueue();
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                assertEquals(new Pair<>(-n, n), lpq2.currentSmallest(primitivePQ));
                lpq2.nextSmallest(primitivePQ);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
        assertEquals(0, lpq2.getModifiedNodesState().size());
        assertEquals(this.range, lpq2.dequeueCounter());

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                assertEquals(new Pair<>(-n, n), primitivePQ.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
    }

    @Test
    public void testExportNodesToArray() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = lpq.enqueue(n, n);
            try {
                lpq.testHeapInvariantRecursive();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
            assertEquals(1, newRoot.getIndex());
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, lpq.size());

        PQNode nodesArr[] = lpq.exportNodesToArray();

        assertEquals(0, lpq.size());
        assertTrue(lpq.isEmpty());
        try {
            lpq.top();
            fail("LocalPriorityQueue should be empty");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
        }

        IntStream.range(0, this.range).forEach(n -> {
            assertEquals(null, nodesArr[n].getFather());
            assertEquals(null, nodesArr[n].getLeft());
            assertEquals(null, nodesArr[n].getLeft());
        });
    }
}
