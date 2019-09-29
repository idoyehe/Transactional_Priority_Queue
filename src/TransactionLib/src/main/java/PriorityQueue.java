package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PriorityQueue {
    private static final long singletonMask = 0x4000000000000000L;
    private static final long versionNegMask = singletonMask;
    private LockQueue pqLock = new LockQueue();
    public LocalPriorityQueue root = new LocalPriorityQueue();// the actual nodes are stored in primitive local queue TODO: change to protected for tests
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
        pqLock.lock();
    }

    protected boolean tryLock() {
        return pqLock.tryLock();
    }

    protected void unlock() {
        pqLock.unlock();
    }

    protected void enqueueNodes(LocalPriorityQueue lPQueue) {
        assert (lPQueue != null);
        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue enqueue Nodes");
        }
        try {
            while (!lPQueue.isEmpty()) {
                if (TX.DEBUG_MODE_QUEUE) {
                    System.out.println("Priority Queue enqueueNodes - lPQueue is not empty");
                }
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
                    System.out.println("Priority Queue dequeueNodes - priority queue is empty");
                }
            }
        }
    }

    public void enqueue(Comparable priority, Object value) throws TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue enqueue - singleton");
            }

            this.lock();
            this.root.enqueue(priority, value);

            this.setVersion(TX.getVersion());
            this.setSingleton(true);

            this.unlock();
            return;
        }

        // TX

        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue enqueue - in TX");
        }

        if (localStorage.readVersion < getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        if (lPQueue == null) {//First time to enqueue the PriorityQueue
            lPQueue = new LocalPriorityQueue();
        }
        lPQueue.enqueue(priority, value);
        pqMap.put(this, lPQueue);
    }


    public boolean isEmpty() throws TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue isEmpty - singleton");
            }
            lock();
            int ret = root.size;//TODO: should be after lock or before lock as Queue?
            setVersion(TX.getVersion());
            setSingleton(true);
            unlock();
            return (ret <= 0);
        }

        // TX
        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue isEmpty - in TX");
        }

        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue isEmpty - now not locked by me");
        }

        if (localStorage.readVersion < getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (!tryLock()) { // if priority queue is locked by another thread
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue isEmpty - couldn't lock");
            }
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        // now we have the lock
        if (this.root.size > 0) {
            return false;
        }
        assert this.root.size <= 0;
        // check lPQueue
        HashMap<PriorityQueue, LocalPriorityQueue> qMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = qMap.get(this);
        if (lPQueue == null) {//TODO: is it needed to crate local priority queue now?
            lPQueue = new LocalPriorityQueue();
        }
        qMap.put(this, lPQueue);
        return lPQueue.isEmpty();
    }

    public Pair<Comparable, Object> dequeue() throws TXLibExceptions.PQueueIsEmptyException, TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeue - singleton");
            }

            lock();
            Pair<Comparable, Object> ret = this.root.dequeue();
            setVersion(TX.getVersion());
            setSingleton(true);
            unlock();
            return ret;
        }

        // TX

        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }

        if (localStorage.readVersion < getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (!tryLock()) { // if queue is locked by another thread
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        // now we have the lock
        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        if (lPQueue == null) {
            lPQueue = new LocalPriorityQueue();
        }

        Pair<Comparable, Object> pQueueMin = null;
        try {
            pQueueMin = this.root.k_th_smallest(lPQueue.dequeueCounter + 1);
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeue - priority queue is not has minimum");
            }
        }

        Pair<Comparable, Object> lPQueueMin = null;

        try {
            lPQueueMin = lPQueue.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.getKey().compareTo(lPQueueMin.getKey()) < 0)) {// the minimum node is in the priority queue
            lPQueue.dequeueCounter++;//mark the dequeue from the priority queue
            return pQueueMin;
        }
        // the minimum node is in the local priority queue
        lPQueue.dequeue();// can throw an exception
        pqMap.put(this, lPQueue);
        return lPQueueMin;
    }

    public Pair<Comparable, Object> top() throws TXLibExceptions.PQueueIsEmptyException, TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue top - singleton");
            }

            lock();
            Pair<Comparable, Object> ret = this.root.top();
            setVersion(TX.getVersion());
            setSingleton(true);
            unlock();
            return ret;
        }

        // TX

        if (TX.DEBUG_MODE_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }

        if (localStorage.readVersion < getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (!tryLock()) { // if queue is locked by another thread
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        // now we have the lock
        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        if (lPQueue == null) {
            lPQueue = new LocalPriorityQueue();
        }

        Pair<Comparable, Object> pQueueMin = null;
        try {
            pQueueMin = this.root.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeue - priority queue is not has minimum");
            }
        }

        Pair<Comparable, Object> lPQueueMin = null;

        try {
            lPQueueMin = lPQueue.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.getKey().compareTo(lPQueueMin.getKey()) < 0)) {// the minimum node is in the priority queue
            return pQueueMin;
        }
        // the minimum node is in the local priority queue
        return lPQueue.top();// can throw an exception
    }
}

