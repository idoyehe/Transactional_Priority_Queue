package TransactionLib.src.test.java;

import TransactionLib.src.main.java.*;

import org.junit.Test;

import static junit.framework.TestCase.*;
import static junit.framework.TestCase.fail;

import java.util.stream.IntStream;


public class LocalPriorityQueueTest {
    final int range = 4;

    @Test
    public void testLocalPriorityQueueConstructor() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTopWithClearIgnoredWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.topWithClearIgnored();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testTopWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.topWithClearIgnored();
    }

    @Test(expected = TXLibExceptions.PQueueIsEmptyException.class)
    public void testDequeueWhenQueueEmpty() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        lpq.dequeue();
    }

    @Test
    public void testSingleEnqueue() throws TXLibExceptions.PQueueIsEmptyException {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        final PQObject element = new PQObject(1, 1);

        final PQObject enqueuedNode = lpq.enqueue(element.getPriority(), element.getValue());
        assertEquals(element.getPriority(), lpq.top().getPriority());
        assertEquals(element.getValue(), lpq.top().getValue());
        assertFalse(element.getIsIgnored());
        assertEquals(1, lpq.size());

        assertEquals(0, enqueuedNode.getIndex());
        assertTrue(lpq.containsNode(enqueuedNode));
    }

    @Test
    public void testSingleEnqueueAndSingleDequeue() throws TXLibExceptions.PQueueIsEmptyException {
        final PQObject element = new PQObject(1, 1);
        LocalPriorityQueue lpq = new LocalPriorityQueue();

        final PQObject enqueuedNode = lpq.enqueue(element.getPriority(), element.getValue());
        assertEquals(element.getPriority(), lpq.top().getPriority());
        assertEquals(element.getValue(), lpq.top().getValue());

        assertFalse(element.getIsIgnored());
        assertEquals(1, lpq.size());

        assertEquals(0, enqueuedNode.getIndex());
        assertTrue(lpq.containsNode(enqueuedNode));

        assertEquals(enqueuedNode, lpq.dequeue());
    }

    @Test
    public void testMultiplyEnqueueAndMultiplyDequeue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        PQObject nodesArray[] = new PQObject[this.range];

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            nodesArray[n] = lpq.enqueue(n, n);
            assertEquals(n, nodesArray[n].getPriority());
            assertEquals(0, nodesArray[n].getIndex());
        });
        assertEquals(this.range, lpq.size());

        IntStream.range(0, this.range).forEach(n -> {
            assertTrue(lpq.containsNode(nodesArray[n]));
        });

        IntStream.range(0, this.range).forEach(n -> {
            try {
                final PQObject element = new PQObject(n, n);
                assertTrue(element.isContentEqual(lpq.top()));

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
        PQObject nodesArr[] = new PQObject[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = lpq.enqueue(n, n);
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
                assertEquals(-n, lpq.top().getPriority());
                assertEquals(n, lpq.top().getValue());
                assertEquals(nodesArr[n], lpq.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
        });

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = lpq.enqueue(n, n);
            nodesArr[n] = newRoot;

            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
        });

        for (int i = 0; i < nodesArr.length; i++) {
            assertEquals(i, nodesArr[i].getPriority());
            assertEquals(i, nodesArr[i].getValue());
        }


        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            Integer oldPrio = (Integer) nodesArr[n].getPriority();
            lpq.decreasePriority(nodesArr[n], 2 * oldPrio);
            assertEquals(oldPrio, nodesArr[n].getPriority());
        });

        IntStream.range(0, this.range).forEach(n -> {
            try {
                final PQObject element = new PQObject(n, n);
                assertTrue(element.isContentEqual(lpq.top()));
                assertTrue(nodesArr[n].isContentEqual(lpq.top()));
                assertEquals(nodesArr[n], lpq.dequeue());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("LocalPriorityQueue should not be empty");
            }
        });
    }

    @Test
    public void testIgnoringNodes1() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
        PQObject nodesArr[] = new PQObject[this.range];
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            nodesArr[n] = lpq.enqueue(n, n);

            if (n % 2 == 1) {
                nodesArr[n].setIgnored();
                lpq.incrementIgnoredCounter();
            }

            assertEquals(n, nodesArr[n].getValue());
            assertEquals(n, nodesArr[n].getPriority());
            assertEquals(n % 2 == 1, nodesArr[n].getIsIgnored());
        });

        assertEquals(this.range / 2, lpq.size());
        IntStream.range(0, this.range).forEach(n -> {
            try {
                PQObject element = lpq.topWithClearIgnored();
                assertEquals(nodesArr[n * 2], element);

                element = lpq.dequeue();
                assertEquals(nodesArr[n * 2], element);

                if (n >= this.range / 2) {
                    fail("Should raise exception the LPQueue is empty");
                }
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                if (n < this.range / 2) {
                    fail("Should not raise exception the LPQueue is empty");
                }
            }
        });
    }

    @Test
    public void testSmallestSimulation() {
        PrimitivePriorityQueue anotherPrimitive = new PrimitivePriorityQueue();

        PQObject nodesArr[] = new PQObject[this.range * 2];
        IntStream.range(0, this.range * 2).map(i -> this.range * 2 - 1 - i).forEach(n -> {
            nodesArr[n] = anotherPrimitive.enqueue(n, n);

            if (n % 2 == 1) {
                nodesArr[n].setIgnored();
                anotherPrimitive.incrementIgnoredCounter();
            }

            assertEquals(n, nodesArr[n].getValue());
            assertEquals(n, nodesArr[n].getPriority());
            assertEquals(n % 2 == 1, nodesArr[n].getIsIgnored());
        });

        assertEquals(this.range, anotherPrimitive.size());

        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).forEachOrdered(n -> {
            try {
                assertTrue(new PQObject(n * 2, n * 2).isContentEqual(lpq.currentSmallest(anotherPrimitive)));
                lpq.nextSmallest(anotherPrimitive);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
        });

        assertEquals(0, lpq.getIgnoredElementsState().size());

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
            lpq2.addModifiedElementFromState(nodesArr[0]);

            //checking also ignoring global ignored is nodesArr[1] and also local modifications
            assertTrue(nodesArr[2].isContentEqual(lpq2.currentSmallest(anotherPrimitive)));

            lpq2.nextSmallest(anotherPrimitive);

            //checking also ignoring global ignored is nodesArr[3] and also local modifications
            assertTrue(nodesArr[4].isContentEqual(lpq2.currentSmallest(anotherPrimitive)));


        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            e.printStackTrace();
            fail("Local priority queue should not be empty");
        }

        final LocalPriorityQueue lpq3 = new LocalPriorityQueue();
        try {
            assertEquals(nodesArr[0], anotherPrimitive.dequeue());
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
            if (i % 2 == 0) {
                assertFalse(nodesArr[i].getIsIgnored());
                anotherPrimitive.decreasePriority(nodesArr[i], -(Integer) nodesArr[i].getPriority());
                lpq3.addModifiedElementFromState(nodesArr[i]);
            } else {
                assertTrue(nodesArr[i].getIsIgnored());
            }
        }
        for (int i = 0; i < nodesArr.length / 2; i++) {
            try {
                assertEquals(nodesArr[nodesArr.length - 2], anotherPrimitive.top());
                if (i % 2 == 0) {
                    assertTrue(new PQObject(i, i).isContentEqual(lpq3.currentSmallest(anotherPrimitive)));
                }
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
        PQObject nodesArray[] = new PQObject[this.range * 2];

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = primitivePQ.enqueue(n, n);
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
        IntStream.range(-2 * this.range, -this.range).forEach(n -> {
            nodesArray[n + this.range * 3] = primitivePQ.enqueue(n, n);
            nodesArray[n + this.range * 3].setIgnored();
            primitivePQ.incrementIgnoredCounter();
            try {
                assertEquals(nodesArray[this.range], primitivePQ.top());
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                fail("Local priority queue should not be empty");
            }
            assertEquals(n, nodesArray[n + this.range * 3].getPriority());
            assertEquals(n, nodesArray[n + this.range * 3].getValue());
        });
        assertEquals(this.range, primitivePQ.size());


        final LocalPriorityQueue lpq = new LocalPriorityQueue();

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            primitivePQ.decreasePriority(nodesArray[n], -n);
            lpq.addModifiedElementFromState(nodesArray[n]);
            assertEquals(-n, nodesArray[n].getPriority());
        });
        assertEquals(this.range, lpq.getIgnoredElementsState().size());
        assertEquals(0, lpq.dequeueCounter());

        try {
            lpq.currentSmallest(primitivePQ);
            fail("should throw empty Queue Exception");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
        }
        assertEquals(0, lpq.getIgnoredElementsState().size());
        assertEquals(2 * this.range, lpq.dequeueCounter());

        final LocalPriorityQueue lpq2 = new LocalPriorityQueue();
        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                assertTrue(new PQObject(-n, n).isContentEqual(lpq2.currentSmallest(primitivePQ)));
                lpq2.nextSmallest(primitivePQ);
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
        assertEquals(0, lpq2.getIgnoredElementsState().size());
        assertEquals(this.range * 2, lpq2.dequeueCounter());

        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            try {
                assertTrue(new PQObject(-n, n).isContentEqual(primitivePQ.dequeue()));
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                fail("should not throw empty Queue Exception");
            }
        });
    }

    @Test
    public void testMerging2LocalPriorityQueue() throws TXLibExceptions.PQueueIsEmptyException, Exception {
        LocalPriorityQueue lpq1 = new LocalPriorityQueue();
        LocalPriorityQueue lpq2 = new LocalPriorityQueue();
        PQObject nodesArray[] = new PQObject[this.range * 2];


        IntStream.range(0, this.range).map(i -> this.range - 1 - i).forEach(n -> {
            final PQObject newRoot = lpq1.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
            nodesArray[n] = newRoot;
            if (n % 2 == 1) {
                newRoot.setIgnored();
                lpq1.incrementIgnoredCounter();
            } else {
                lpq2.addModifiedElementFromState(newRoot);
            }

        });
        assertEquals(this.range / 2, lpq1.size());

        IntStream.range(this.range, this.range * 2).map(i -> this.range * 2 - 1 - (i - this.range)).forEach(n -> {
            final PQObject newRoot = lpq2.enqueue(n, n);
            assertEquals(n, newRoot.getPriority());
            assertEquals(n, newRoot.getValue());
            nodesArray[n] = newRoot;
        });
        assertEquals(this.range, lpq2.size());

        lpq2.mergingPriorityQueues(lpq1);
        assertEquals(this.range, lpq1.size());
        assertEquals(0, lpq2.size());

        try {
            lpq2.top();
            fail("should throw empty Queue Exception");
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            assertTrue(e != null);
        }
    }
}
