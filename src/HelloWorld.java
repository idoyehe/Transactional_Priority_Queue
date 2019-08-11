public class HelloWorld implements Runnable {
    public void run() {
        System.out.println("Hello world from thread number " +
                Thread.currentThread().getName());
    }

    public static void main(String[] args) {
        Thread[] threads = new Thread[10]; // create an array of threads
        for (int i = 0; i < 10; i++) {
            String threadName = Integer.toString(i);
            threads[i] = new Thread(new HelloWorld(), threadName); // create threads
        }
        for (Thread thread : threads) {
            thread.start(); // start the threads
        }
        for (Thread thread : threads) {
            try {
                thread.join(); // wait for the threads to terminate
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("That's all, folks");
    }
}