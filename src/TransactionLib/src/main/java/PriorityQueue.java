package TransactionLib.src.main.java;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    void commitLocalChanges(LocalPriorityQueue lPQueue) {
        assert (lPQueue != null);
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges");
        }
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges - dequeue nodes");
        }
        this.dequeueNodes(lPQueue.dequeueCounter());

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue commitLocalChanges - merge old nodes to new nodes");
        }

        lPQueue.mergingPriorityQueues(this.internalPriorityQueue);//the global queue is merged into the local queue

        assert this.internalPriorityQueue.size() == 0;
        this.internalPriorityQueue = lPQueue;//global queue is now the local queue

    }

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
                this.internalPriorityQueue.singleDequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                e.printStackTrace();
                if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                    System.out.println("Priority Queue dequeueNodes - priority queue is empty");
                }
            }
        }
    }

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
            pqMap.put(this, lPQueue);
            return newNode;
        }
        return nodeToModify;
    }


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

    public boolean isEmpty() {
        return this.size() <= 0;
    }

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

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.compareTo(lPQueueMin) < 0)) {// the minimum node is in the priority queue
            lPQueue.nextSmallest(this.internalPriorityQueue);
            pqMap.put(this, lPQueue);
            return pQueueMin.getValue();
        }
        // the minimum node is in the local priority queue
        lPQueue.dequeue();// can throw an exception
        pqMap.put(this, lPQueue);
        return lPQueueMin.getValue();
    }

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

