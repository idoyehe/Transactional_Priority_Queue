package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.concurrent.atomic.AtomicLong;

public class PriorityQueue {
    //	private static final long lockMask = 0x2000000000000000L;
    private static final long singletonMask = 0x4000000000000000L;
    //	private static final long versionMask = lockMask | singletonMask;
    private static final long versionNegMask = singletonMask;
    private LockQueue qLock = new LockQueue();
    private LocalPriorityQueue root;// the actual nodes are stored in primitive local queue
    // bit 61 is lock
    // bit 62 is singleton
    // 0 is false, 1 is true
    // we are missing a bit because this is signed
    private AtomicLong versionAndFlags = new AtomicLong(); //This is the VC of the queue

    protected long getVersion() {
        return (versionAndFlags.get() & (~versionNegMask));
    }

    protected void setVersion(long version) {
        long l = versionAndFlags.get();
//		assert ((l & lockMask) != 0);
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        versionAndFlags.set(l);
    }

    protected boolean isSingleton() {
        long l = versionAndFlags.get();
        return (l & singletonMask) != 0;
    }

    protected void setSingleton(boolean value) {//TODO: what is the meaning of singletone here?
        long l = versionAndFlags.get();
//		assert ((l & lockMask) != 0);
        if (value) {
            l |= singletonMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~singletonMask);
        versionAndFlags.set(l);
    }

    private void lock() {
        qLock.lock();
    }

    protected boolean tryLock() {
        return qLock.tryLock();
    }

    protected void unlock() {
        qLock.unlock();
    }

    protected void enqueueNodes(LocalPriorityQueue lPQueue) {
        assert (lPQueue != null);
        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue enqueueNodes");
        }
        try {
            while (!lPQueue.isEmpty()) {
                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("Priority Queue enqueueNodes - lPQueue is not empty");
                }
                PQNode node = new PQNode();
                Pair<Comparable, Object> prioValuePair = lPQueue.dequeue();


                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("Priority Queue enqueueNodes - lPQueue node priority is " + prioValuePair.getKey());
                    System.out.println("Priority Queue enqueueNodes - lPQueue node value is " + prioValuePair.getValue());

                }
                this.root.enqueue(prioValuePair.getKey(), prioValuePair.getValue());

            }
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue enqueueNodes - local queue is empty");
            }
        } catch (TXLibExceptions.PQIndexNotFound e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue enqueueNodes - Index not found");
            }
        }
    }

    protected void dequeueNodes(int dequeueCounter) {

        if (dequeueCounter == 0) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeueNodes - dequeueCounter is 0");
            }
            return;
        }

        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue dequeueNodes");
        }
        for (int i = 0; i < dequeueCounter; i++) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeueNodes - dequeueing");
            }
            try {
                this.root.dequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("Priority Queue dequeueNodes - local queue is empty");
                }
            } catch (TXLibExceptions.PQIndexNotFound e) {
                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("Priority Queue dequeueNodes - Index not found");
                }
            }
        }
    }
}

