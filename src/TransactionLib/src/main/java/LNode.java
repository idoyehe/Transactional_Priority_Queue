package TransactionLib.src.main.java;

import java.util.concurrent.atomic.AtomicLong;

public class LNode {
    private static final long lockMask = 0x1000000000000000L;//mask that indicate if LNode is locked
    private static final long deleteMask = 0x2000000000000000L;//mask that indicate if LNode is deleted
    private static final long singletonMask = 0x4000000000000000L;
    private static final long versionNegMask = lockMask | deleteMask | singletonMask;
    public LNode next = null;//TODO:change back to protected
    public Integer key = null; // TODO use templates //TODO:change back to protected
    protected Object val = null; // TODO maybe use templates
    // bit 60 is lock
    // bit 61 is deleted
    // bit 62 is singleton
    // 0 is false, 1 is true
    // we are missing a bit because this is signed
    private AtomicLong versionAndFlags = new AtomicLong();//each LNode holds it's version

    protected boolean tryLock() {
        long l = versionAndFlags.get();
        if ((l & lockMask) != 0) {//indicte node is already locked
            return false;
        }
        long locked = l | lockMask;
        return versionAndFlags.compareAndSet(l, locked);//always set ?
    }

    protected void unlock() {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);//assertting node is already locked
        long unlocked = l & (~lockMask);
        boolean ret = versionAndFlags.compareAndSet(l, unlocked);//always set ?
        assert (ret);
    }

    protected boolean isLocked() {
        long l = versionAndFlags.get();
        return (l & lockMask) != 0;
    }

    protected boolean isDeleted() {
        long l = versionAndFlags.get();
        return (l & deleteMask) != 0;
    }

    protected void setDeleted(boolean value) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);//assertting node is not deleted
        if (value) {
            l |= deleteMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~deleteMask);
        versionAndFlags.set(l);
    }

    protected boolean isLockedOrDeleted() {
        long l = versionAndFlags.get();
        return ((l & deleteMask) != 0) || ((l & lockMask) != 0);
    }

    protected boolean isSingleton() {
        long l = versionAndFlags.get();
        return (l & singletonMask) != 0;
    }

    protected void setSingleton(boolean value) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);
        if (value) {
            l |= singletonMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~singletonMask);
        versionAndFlags.set(l);
    }

    protected void setSingletonNoLockAssert(boolean value) {
        long l = versionAndFlags.get();
        if (value) {
            l |= singletonMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~singletonMask);
        versionAndFlags.set(l);
    }

    protected long getVersion() {
        return (versionAndFlags.get() & (~versionNegMask));
    }

    protected void setVersion(long version) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);//assertting node is already locked
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        versionAndFlags.set(l);
    }

    protected boolean isSameVersionAndSingleton(long version) {
        long l = versionAndFlags.get();
        if ((l & singletonMask) != 0) {
            l &= (~versionNegMask);
            return l == version;
        }
        return false;
    }

    protected void setVersionAndSingleton(long version, boolean value) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        if (value) {
            l |= singletonMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~singletonMask);
        versionAndFlags.set(l);
    }

    protected void setVersionAndSingletonNoLockAssert(long version, boolean value) {
        long l = versionAndFlags.get();
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        if (value) {
            l |= singletonMask;
            versionAndFlags.set(l);
            return;
        }
        l &= (~singletonMask);
        versionAndFlags.set(l);
    }

    protected void setVersionAndDeletedAndSingleton(long version, boolean deleted, boolean singleton) {
        long l = versionAndFlags.get();
        assert ((l & lockMask) != 0);
        l &= versionNegMask;
        l |= (version & (~versionNegMask));
        if (singleton) {
            l |= singletonMask;
        } else {
            l &= (~singletonMask);
        }
        if (deleted) {
            l |= deleteMask;
        } else {
            l &= (~deleteMask);
        }
        versionAndFlags.set(l);
    }

}
