package TransactionLib.src.main.java;

public class TXLibExceptions {

    @SuppressWarnings("serial")
    public class QueueIsEmptyException extends Exception {

    }

    @SuppressWarnings("serial")
    public class PQueueIsEmptyException extends Exception {

    }

    @SuppressWarnings("serial")
    public class PQIndexNotFound extends Exception {
    }


    @SuppressWarnings("serial")
    public class AbortException extends RuntimeException {

    }

}
