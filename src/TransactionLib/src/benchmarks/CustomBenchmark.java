package TransactionLib.src.benchmarks;

import TransactionLib.src.main.java.*;
import org.junit.Test;

import static junit.framework.TestCase.*;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.IntStream;

public class CustomBenchmark {
    @Test
    public void testLocalPriorityQueueConstructor() {
        LocalPriorityQueue lpq = new LocalPriorityQueue();
    }
}
