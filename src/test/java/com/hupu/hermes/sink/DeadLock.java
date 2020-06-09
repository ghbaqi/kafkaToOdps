package com.hupu.hermes.sink;


import java.security.SecureRandom;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class DeadLock {
    private final ReentrantLock lock = new ReentrantLock();
    private static final SecureRandom random = new SecureRandom();

    public DeadLock() {
    }

    public void runWork() {

        final ThreadPoolExecutor threadpool = new ThreadPoolExecutor(3, 3, 60L, TimeUnit.DAYS,
                new SynchronousQueue<Runnable>(), new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            public Thread newThread(final Runnable r) {
                return new Thread(r, "thread-sn-" + counter.getAndIncrement());
            }
        });
        threadpool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        Thread.currentThread().setName("main-thread");
        for (int i = 0; i < 15; ++i) {
            threadpool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        int timeout = 0;
                        while ((timeout = random.nextInt()) <= 0) {
                        }
                        timeout = timeout % 111;
                        Thread.sleep(timeout * 100L);

                        lock.lock();
                        callLongTime();

                    }
                    catch (final Exception e) {
                        e.printStackTrace();
                    }
                    finally {
                        lock.unlock();
                    }
                }
            });
        }
        threadpool.shutdown();
    }

    public static void main(final String[] args) throws Exception {
        new DeadLock().runWork();
    }

    static long callLongTime() {
        System.out.println("thread name " + Thread.currentThread().getName());
        long sum = 0;
        for (long i = 0; i < 10000000000L; ++i) {
            sum = sum ^ i + i;
        }
        return sum;
    }
}
