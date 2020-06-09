package com.hupu.hermes.sink;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class ThreadPoolTest {

    //    @Autowired
    private ScheduledThreadPoolExecutor executor;

    @Test
    public void t() throws InterruptedException {


        executor = new ScheduledThreadPoolExecutor(1);
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //配置核心线程数
//        executor.setCorePoolSize(5);
        executor.setKeepAliveTime(11, TimeUnit.MINUTES);
        //配置最大线程数
//        executor.setMaxPoolSize(10);
        //配置队列大小
//        executor.setQueueCapacity(100);
        //配置线程池中的线程的名称前缀
        executor.setMaximumPoolSize(5);
        executor.setThreadFactory(r -> new Thread("sessionCommitThread-"));
        // 设置拒绝策略：当pool已经达到max size的时候，如何处理新任务
        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        System.out.println("start");
        executor.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println("111");
            }
        }, 5, TimeUnit.SECONDS);

        Thread.sleep(111111111L);
    }


    public static void main(String[] args) throws InterruptedException {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
//        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        //配置核心线程数
//        executor.setCorePoolSize(5);
        executor.setKeepAliveTime(11, TimeUnit.MINUTES);
        //配置最大线程数
//        executor.setMaxPoolSize(10);
        //配置队列大小
//        executor.setQueueCapacity(100);
        //配置线程池中的线程的名称前缀
        executor.setMaximumPoolSize(5);
        executor.setThreadFactory(r -> new Thread("sessionCommitThread-"));
        // 设置拒绝策略：当pool已经达到max size的时候，如何处理新任务
        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        System.out.println("start");
        executor.schedule(() -> System.out.println("111"), 5L, TimeUnit.SECONDS);

        Thread.sleep(111111111L);
    }


}
