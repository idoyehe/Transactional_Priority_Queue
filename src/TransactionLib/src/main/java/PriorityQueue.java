package TransactionLib.src.main.java;

import javafx.util.Pair;

import java.util.ArrayList;
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
        this.dequeueNodes(lPQueue.dequeueCounter());
        PQNode[] oldExportedPQueue = this.internalPriorityQueue.exportNodesToArray();
        PQNode[] newExportedPQueue = lPQueue.exportNodesToArray();
        ArrayList<PQNode> modifiedNodesState = lPQueue.getModifiedNodesState();
        this.mergingNewNodes(oldExportedPQueue, newExportedPQueue, modifiedNodesState);
    }

    private void mergingNewNodes(PQNode[] oldExportedPQueue, PQNode[] newExportedPQueue, ArrayList<PQNode> modifiedNodesState) {
        assert (oldExportedPQueue != null);
        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue enqueue Nodes");
        }
        int modifiedCounter = modifiedNodesState.size();
        for (int i = 0; i < oldExportedPQueue.length; i++) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue mergingNewNodes - merge old nodes");
            }
            PQNode nodeToEnqueue = oldExportedPQueue[i];
            oldExportedPQueue[i] = null;
            assert nodeToEnqueue.getFather() == null && nodeToEnqueue.getRight() == null && nodeToEnqueue.getLeft() == null;
            if (modifiedNodesState.contains(nodeToEnqueue)) {
                if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                    System.out.println("Priority Queue mergingNewNodes - node has been modified therefore ignored");
                }
                modifiedNodesState.remove(nodeToEnqueue);
                continue;
            }
            this.internalPriorityQueue.enqueueAsNode(nodeToEnqueue);
        }

        for (int i = 0; i < newExportedPQueue.length; i++) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue mergingNewNodes - merge new nodes");
            }
            PQNode nodeToEnqueue = newExportedPQueue[i];
            newExportedPQueue[i] = null;
            assert nodeToEnqueue.getFather() == null && nodeToEnqueue.getRight() == null && nodeToEnqueue.getLeft() == null;
            assert !modifiedNodesState.contains(nodeToEnqueue);
            this.internalPriorityQueue.enqueueAsNode(nodeToEnqueue);
        }

        assert modifiedNodesState.size() == 0;//handled in all modified nodes
        assert this.internalPriorityQueue.size() == newExportedPQueue.length + oldExportedPQueue.length - modifiedCounter;

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
                this.internalPriorityQueue.dequeue();
            } catch (TXLibExceptions.PQueueIsEmptyException e) {
                if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                    System.out.println("Priority Queue dequeueNodes - priority queue is empty");
                }
            }
        }
    }

    public final PQNode enqueue(Comparable priority, Object value) throws TXLibExceptions.AbortException {

        LocalStorage localStorage = TX.lStorage.get();

        // SINGLETON
        if (!localStorage.TX) {

            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue enqueue - singleton");
            }

            this.lock();
            final PQNode newNode = this.internalPriorityQueue.enqueue(priority, value);

            this.setVersion(TX.getVersion());
            this.setSingleton(true);

            this.unlock();
            return newNode;
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue enqueue - in TX");
        }

        if (localStorage.readVersion < this.getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == this.getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

        HashMap<PriorityQueue, LocalPriorityQueue> pqMap = localStorage.priorityQueueMap;
        LocalPriorityQueue lPQueue = pqMap.get(this);

        localStorage.readOnly = false;

        if (lPQueue == null) {//First time to enqueue the PriorityQueue
            lPQueue = new LocalPriorityQueue();
        }
        final PQNode newNode = lPQueue.enqueue(priority, value);
        pqMap.put(this, lPQueue);
        return newNode;
    }

    public PQNode decreasePriority(final PQNode nodeToModify, Comparable newPriority) throws TXLibExceptions.AbortException {
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

        if (localStorage.readVersion < this.getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == this.getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

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

        if (newPriority.compareTo(nodeToModify.getPriority()) < 0 && this.internalPriorityQueue.containsNode(nodeToModify)) {
            lPQueue.addModifiedNode(nodeToModify);
            PQNode newNode = lPQueue.enqueue(newPriority, nodeToModify.getValue());
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

        if (localStorage.readVersion < this.getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == this.getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

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
        assert this.internalPriorityQueue.size() - lPQueue.dequeueCounter() >= 0;
        assert lPQueue.size() >= 0;
        return this.internalPriorityQueue.size() - lPQueue.dequeueCounter() - lPQueue.modifiedNodesCounter() + lPQueue.size();
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
            Pair<Comparable, Object> ret = this.internalPriorityQueue.dequeue();
            setVersion(TX.getVersion());
            setSingleton(true);
            this.unlock();
            return ret.getValue();
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - yet not locked by me");
        }

        if (localStorage.readVersion < this.getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == this.getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

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

        Pair<Comparable, Object> pQueueMin = null;

        try {
            pQueueMin = lPQueue.currentSmallest(this.internalPriorityQueue);
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - transactional priority queue is empty");
            }
        }

        Pair<Comparable, Object> lPQueueMin = null;

        try {
            lPQueueMin = lPQueue.top();
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - dequeue from local queue");
            }
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.getKey().compareTo(lPQueueMin.getKey()) < 0)) {// the minimum node is in the priority queue
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
            Pair<Comparable, Object> ret = this.internalPriorityQueue.top();
            setVersion(TX.getVersion());
            setSingleton(true);
            this.unlock();
            return ret.getValue();
        }

        // TX

        if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
            System.out.println("Priority Queue dequeue - in TX");
        }

        if (localStorage.readVersion < this.getVersion()) {
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }
        if ((localStorage.readVersion == this.getVersion()) && (isSingleton())) {
            TX.incrementAndGetVersion();
            localStorage.TX = false;
            TXLibExceptions excep = new TXLibExceptions();
            throw excep.new AbortException();
        }

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

        Pair<Comparable, Object> pQueueMin = null;
        try {
            pQueueMin = lPQueue.currentSmallest(this.internalPriorityQueue);
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - priority queue is not has minimum");
            }
        }

        Pair<Comparable, Object> lPQueueMin = null;

        try {
            lPQueueMin = lPQueue.top();
        } catch (TXLibExceptions.PQueueIsEmptyException e) {
            if (TX.DEBUG_MODE_PRIORITY_QUEUE) {
                System.out.println("Priority Queue dequeue - local queue is empty");
            }
        }

        if (pQueueMin != null && (lPQueueMin == null || pQueueMin.getKey().compareTo(lPQueueMin.getKey()) < 0)) {// the minimum node is in the priority queue
            return pQueueMin.getValue();
        }
        // the minimum node is in the local priority queue
        return lPQueue.top().getValue();// can throw an exception
    }
}

