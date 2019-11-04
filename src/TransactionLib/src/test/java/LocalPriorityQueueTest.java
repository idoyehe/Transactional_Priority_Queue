package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import org.junit.Test;

import static junit.framework.TestCase.*;
import static junit.framework.TestCase.fail;

import java.util.function.Predicate;
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
    public void testSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException {
        final PQNode element = new PQNode(1, 1);
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.enqueue(element.getPriority(), element.getValue());
        assertEquals(element.getPriority(), lpq.top().getPriority());
        assertEquals(element.getValue(), lpq.top().getValue());
        assertEquals(1, lpq.size());
    }

    @Test
    public void testSingleEnqueueAndSingleDequeue() throws TXLibExceptions.PQueueIsEmptyException {
        final PQNode element = new PQNode(1, 1);
        LocalPriorityQueue lpq = new LocalPriorityQueue();

        lpq.enqueue(element.getPriority(), element.getValue());

        assertEquals(element.getPriority(), lpq.top().getPriority());
        assertEquals(element.getValue(), lpq.top().getValue());

        assertEquals(1, lpq.size());
        assertTrue(element.isContentEqual(lpq.dequeue()));
    }

    @Test
    public void testMultiplyEnqueueAndMultiplyDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = lpq.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
        });
        assertEquals(this.range, lpq.size());

        IntStream.range(0, this.range).forEach(n -> {
            try {
                final PQNode element = new PQNode(n, n);
                assertEquals(element.getPriority(), lpq.top().getPriority());
                assertEquals(element.getValue(), lpq.top().getValue());

                assertTrue(element.isContentEqual(lpq.dequeue()));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
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

            assertEquals(n, newRoot.getValue());
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
            assertEquals(-oldPrio, nodesArr[n].getPriority());
        });

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                final PQNode element = new PQNode(-n, n);
                assertTrue(element.isContentEqual(lpq.top()));
                assertTrue(nodesArr[n].isContentEqual(lpq.top()));
                assertTrue(element.isContentEqual(lpq.dequeue()));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
        });
    }

    @Test
    public void testSmallestSimulation() {
        PrimitivePriorityQueue anotherPrimitive = new PrimitivePriorityQueue();

        PQNode nodesArr[] = new PQNode[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = anotherPrimitive.enqueue(n, n);
            assertEquals(n, newRoot.getValue());
            assertEquals(n, newRoot.getPriority());
            nodesArr[n] = newRoot;
        });

        assertEquals(this.range, anotherPrimitive.size());

        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).forEachOrdered(n -> {
            try {
                assertTrue(new PQNode(n, n).isContentEqual(lpq.currentSmallest(anotherPrimitive)));
                lpq.nextSmallest(anotherPrimitive);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
        });
        assertEquals(0, lpq.getModifiedNodesState().size());

        try {
            lpq.currentSmallest(anotherPrimitive);
            fail("Local priority should simulate all dequeued");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assertTrue(e != null);
        }

        try {
            lpq.nextSmallest(anotherPrimitive);
            fail("Local priority should simulate all dequeued");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assertTrue(e != null);
        }

        //decreasing the root
        final LocalPriorityQueue lpq2 = new LocalPriorityQueue();

        try {
            int newPrio = -1;
            assertTrue(nodesArr[0].isContentEqual(anotherPrimitive.top()));
            anotherPrimitive.decreasePriority(nodesArr[0], newPrio);
            assertTrue(nodesArr[0].isContentEqual(anotherPrimitive.top()));
            assertEquals(newPrio, nodesArr[0].getPriority());
            lpq2.addModifiedNode(nodesArr[0]);

            assertTrue(nodesArr[1].isContentEqual(lpq2.currentSmallest(anotherPrimitive)));

            int newPrio2 = 1;
            anotherPrimitive.decreasePriority(nodesArr[1], newPrio2);
            assertEquals(nodesArr[0], anotherPrimitive.top());
            assertEquals(newPrio2, nodesArr[1].getPriority());
            lpq2.addModifiedNode(nodesArr[1]);

            assertTrue(nodesArr[2].isContentEqual(lpq2.currentSmallest(anotherPrimitive)));


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
            assertEquals(nodesArr[0], anotherPrimitive.top());
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
                assertEquals(nodesArr[nodesArr.length - 1], anotherPrimitive.top());
                assertTrue(new PQNode(i, i).isContentEqual(lpq3.currentSmallest(anotherPrimitive)));
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
                assertEquals(nodesArray[n], primitivePQ.top());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
        });

        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            primitivePQ.decreasePriority(nodesArray[n], -n);
            lpq.addModifiedNode(nodesArray[n]);
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
                assertTrue(new PQNode(-n, n).isContentEqual(lpq2.currentSmallest(primitivePQ)));
                lpq2.nextSmallest(primitivePQ);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
        assertEquals(0, lpq2.modifiedNodesCounter());
        assertEquals(this.range, lpq2.dequeueCounter());

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                assertTrue(new PQNode(-n, n).isContentEqual(primitivePQ.dequeue()));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
    }

    @Test
    public void testMerging2LocalPriorityQueue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq1 = new LocalPriorityQueue();
        LocalPriorityQueue lpq2 = new LocalPriorityQueue();
        PQNode nodesArray[] = new PQNode[this.range * 2];


        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQNode newRoot = lpq1.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
            nodesArray[n] = newRoot;
        });
        assertEquals(this.range, lpq1.size());

        IntStream.range(this.range, this.range * 2).map(i -> this.range * 2 - 1 - (i - this.range)).forEach(n -> {
            final PQNode newRoot = lpq2.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
            nodesArray[n] = newRoot;
            lpq1.addModifiedNode(newRoot);
        });
        assertEquals(this.range, lpq2.size());

        lpq1.mergingPriorityQueuesWithoutModification(lpq2);
        assertEquals(this.range, lpq1.size());
        assertEquals(0, lpq2.size());

        try {
            lpq2.top();
            fail("should throw empty Queue Exception");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assertTrue(e != null);
        }

        IntStream.range(this.range, this.range * 2).map(i -> this.range * 2 - 1 - (i - this.range)).forEach(n -> {
            final PQNode newRoot = lpq2.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
            nodesArray[n] = newRoot;

        });
        assertEquals(this.range, lpq2.size());


        lpq1.mergingPriorityQueuesWithoutModification(lpq2);
        assertEquals(this.range * 2, lpq1.size());
        assertEquals(0, lpq2.size());

        IntStream.range(0, this.range * 2).forEach(n -> {
            try {
                assertEquals(nodesArray[n], lpq1.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
    }

    class StaticTrue<PQNode> implements Predicate<PQNode> {

        @Override
        public boolean test(PQNode pqNode) {
            return true;
        }
    }

    class StaticFalse<PQNode> implements Predicate<PQNode> {

        @Override
        public boolean test(PQNode pqNode) {
            return false;
        }
    }
}
