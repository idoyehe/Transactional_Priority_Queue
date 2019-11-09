package TransactionLib.src.main.java;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * this class is the implementation of the transactional priority queue
 * FOR COMPLEXITY CALCULATION THIS SIZE IS N
 */
public class PriorityQueue {
    private static final long singletonMask = 0x4000000000000000L;
    private static final long versionNegMask = singletonMask;
    private LockQueue pqLock = new LockQueue();
    public PrimitivePriorityQueue internalPriorityQueue = new PrimitivePriorityQueue();// the actual nodes are stored in primitive local queue TODO: change to protected for tests
    // bit 61 is lock
    // bit 62 is singleton
    // 0 is false, 1 is true
    // we are missing a bit because this is signed
    private AtomicLong versionAndFlags = new AtomicLong(); //This is the VC of the queue

    long getVersion() {
        return (versionAndFlags.get() & (~versionNegMask));
    }

    void setVersion(long version) {
        long l = versionAndFlags.get();
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        versionAndFlags.set(l);
    }

    boolean isSingleton() {
        long l = versionAndFlags.get();
        return (l & singletonMask) != 0;
    }

    private void validateVersionAndSingleton(LocalStorage localStorage) throws TXLibExceptions.AbortException {
        long thisVersion = this.getVersion();
        long readVersion = localStorage.readVersion;
        if (readVersion < thisVersion) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (readVersion == thisVersion && this.isSingleton()) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
    }


    public void setSingleton(boolean value) {
        long l = versionAndFlags.get();
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

    boolean tryLock() {
        return pqLock.tryLock();
    }

    void unlock() {
        pqLock.unlock();
    }

    /**
     * commit the local state of a transaction to the global state
     *
     * @param lPQueue the local state
     * @Complexity amortized O(D * log N + Q + N * logK)
     */
    void commitLocalChanges(LocalPriorityQueue lPQueue) {
        assert (lPQueue != null);
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges");
        }
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges - dequeue nodes");
        }
        this.dequeueNodes(lPQueue.dequeueCounter());//O(D*log N)

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges - merge old nodes to new nodes");
        }

        lPQueue.mergingPriorityQueues(this.internalPriorityQueue);//the global queue is merged into the local queue

        assert this.internalPriorityQueue.size() == 0;
        this.internalPriorityQueue = lPQueue;//global queue is now the local queue

    }

    /**
     * part of commit local changes, dequeue the local state simulation
     *
     * @param dequeueCounter number of dequeues to be done
     * @Complexity O(D * log N)
     */
    private void dequeueNodes(int dequeueCounter) {

        if (dequeueCounter == 0) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeueNodes - dequeueCounter is 0");
            }
            return;
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeueNodes");
        }
        assert dequeueCounter <= this.internalPriorityQueue.size();
        for (int i = 0; i < dequeueCounter; i++) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeueNodes - dequeueing");
            }
            try {
                this.internalPriorityQueue.dequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                    System.out.println("Priority Queue dequeueNodes - priority queue is empty");
                }
            }
        }
    }

    /**
     * enqueue new element with given priority and value
     *
     * @param priority new element priority
     * @param value    new element value
     * @return a refernce of the enqueue node
     * @throws TXLibExceptions.AbortException
     * @Complexity singleton use amortized O(log N)
     * transaction use amortized O(log k)
     */
    public final PQObject enqueue(Comparable priority, Object value) throws TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue enqueue - singleton");
            }

            this.lock();
            final PQObject newNode = this.internalPriorityQueue.enqueue(priority, value);

            this.setVersion(TX.getVersion());
            this.setSingleton(true);

            this.unlock();
            return newNode;
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue enqueue - in TX");
        }

        this.validateVersionAndSingleton(localStorage);


        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        localStorage.readOnly = false;

        if (lPQueue == null) {//First time to enqueue the PriorityQueue
            lPQueue = new LocalPriorityQueue();
        }
        final PQObject newNode = lPQueue.enqueue(priority, value);
        pqMap.put(this, lPQueue);
        return newNode;
    }

    /**
     * decreasing priority of a specific node
     *
     * @param nodeToModify reference of the specific node
     * @param newPriority  the new priority
     * @return a reference of the modified node
     * @throws TXLibExceptions.AbortException
     * @Complexity singleton use O(log N)
     * transaction use amortized O(log k + log N + Q*log D)
     */
    public PQObject decreasePriority(final PQObject nodeToModify, Comparable newPriority) throws TXLibExceptions.AbortException {
        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue decreasePriority - singleton");
            }

            this.lock();
            this.internalPriorityQueue.decreasePriority(nodeToModify, newPriority);

            this.setVersion(TX.getVersion());
            this.setSingleton(true);

            this.unlock();
            return nodeToModify;
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue decreasePriority - in TX");
        }

        this.validateVersionAndSingleton(localStorage);


        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        localStorage.readOnly = false;

        if (lPQueue == null) {//First time to enqueue the PriorityQueue
            lPQueue = new LocalPriorityQueue();
        }
        if (lPQueue.containsNode(nodeToModify)) {
            lPQueue.decreasePriority(nodeToModify, newPriority);
            pqMap.put(this, lPQueue);
            return nodeToModify;
        }
        //assuming the node is in the the transactional PQueue
        if (!this.tryLock()) { // if priority queue is locked by another thread
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue decreasePriority - couldn't lock");
            }
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue decreasePriority - now locked by me");
        }
        // now we have the lock

        if (nodeToModify.compareTo(newPriority) > 0 && this.internalPriorityQueue.containsNode(nodeToModify)) {
            lPQueue.addModifiedElementFromState(nodeToModify);
            PQObject newNode = lPQueue.enqueue(newPriority, nodeToModify.getValue());
            try {
                lPQueue.currentSmallest(this.internalPriorityQueue);//updating the current smallest because maybe the old current was modified
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
            }
            pqMap.put(this, lPQueue);
            return newNode;
        }
        return nodeToModify;
    }

    /**
     * getter of the size
     *
     * @return the actual size of priority queue
     * @throws TXLibExceptions.AbortException
     * @Complexity O(1)
     */
    public int size() throws TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue size - singleton");
            }
            this.lock();
            int ret = internalPriorityQueue.size();
            setVersion(TX.getVersion());
            setSingleton(true);
            this.unlock();
            return ret;
        }

        // TX
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue size - in TX");
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue size - yet not locked by me");
        }

        this.validateVersionAndSingleton(localStorage);


        if (!this.tryLock()) { // if priority queue is locked by another thread
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue size - couldn't lock");
            }
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue size - now locked by me");
        }

        // now we have the lock

        // check lPQueue
        HashMap<PriorityQueue, LocalPriorityQueue> qMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = qMap.get(this);
        if (lPQueue == null) {
            lPQueue = new LocalPriorityQueue();
        }
        qMap.put(this, lPQueue);
        int pQueueSize = this.internalPriorityQueue.size();
        assert pQueueSize - lPQueue.dequeueCounter() + lPQueue.getIgnoredElemntsState().size() >= 0;
        assert lPQueue.size() >= 0;
        return pQueueSize - lPQueue.dequeueCounter() - lPQueue.getIgnoredElemntsState().size() + lPQueue.size();
    }

    /**
     * predicate the check whether the priority queue is empty
     *
     * @return true iff the priority queue is empty
     * @Complexity O(1)
     */
    public boolean isEmpty() {
        return this.size() <= 0;
    }

    /**
     * dequeue the minimum priority node
     *
     * @return the value of the minimum priority queue
     * @throws TXLibExceptions.AbortException
     * @Complexity singleton use O(log N)
     * transaction use O(Q * log D + log K)
     */
    public Object dequeue() throws TXLibExceptions.PQueueIsEmptyException, TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - singleton");
            }

            this.lock();
            PQObject node = this.internalPriorityQueue.dequeue();
            setVersion(TX.getVersion());
            setSingleton(true);
            this.unlock();
            return node.getValue();
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - yet not locked by me");
        }


        this.validateVersionAndSingleton(localStorage);


        if (!this.tryLock()) { // if queue is locked by another thread
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - now locked by me");
        }

        // now we have the lock
        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        localStorage.readOnly = false;

        if (lPQueue == null) {
            lPQueue = new LocalPriorityQueue();
        }

        PQObject pQueueMin = null;

        try {
            pQueueMin = lPQueue.currentSmallest(this.internalPriorityQueue);// here allocating new PQNode
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - transactional priority queue is empty");
            }
        }

        PQObject lPQueueMin = null;

        try {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - top from local queue");
            }
            lPQueueMin = lPQueue.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.compareTo(lPQueueMin.getPriority()) < 0)) {// the minimum node is in the priority queue
            lPQueue.nextSmallest(this.internalPriorityQueue);
            pqMap.put(this, lPQueue);
            return pQueueMin.getValue();
        }
        // the minimum node is in the local priority queue
        lPQueue.dequeue();// can throw an exception
        pqMap.put(this, lPQueue);
        return lPQueueMin.getValue();
    }

    /**
     * getter of the value of the minimum priority node
     *
     * @return the value of the minimum priority queue
     * @throws TXLibExceptions.AbortException
     * @Complexity singleton use O(1)
     * transaction use O(Q * log D)
     */
    public Object top() throws TXLibExceptions.PQueueIsEmptyException, TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue top - singleton");
            }

            this.lock();
            PQObject ret = this.internalPriorityQueue.top();
            setVersion(TX.getVersion());
            setSingleton(true);
            this.unlock();
            return ret.getValue();
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }


        this.validateVersionAndSingleton(localStorage);


        if (!this.tryLock()) { // if queue is locked by another thread
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

        PQObject pQueueMin = null;
        try {
            pQueueMin = lPQueue.currentSmallest(this.internalPriorityQueue);
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - priority queue is not has minimum");
            }
        }

        PQObject lPQueueMin = null;

        try {
            lPQueueMin = lPQueue.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.compareTo(lPQueueMin) < 0)) {// the minimum node is in the priority queue
            return pQueueMin.getValue();
        }
        // the minimum node is in the local priority queue
        return lPQueue.top().getValue();// can throw an exception
    }
}

