package com.hupu.hermes.sink.kafka;

import com.hupu.hermes.sink.odps.SessionManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

//@Component
@Slf4j
public class DelayQueueManager {

    @Autowired
    private Executor sessionThreadPool;


    //    private DelayQueue<SessionManager.SessionCommitTask> delayQueue = new DelayQueue<>();
//
    public static void main(String[] args) throws InterruptedException {

        ScheduledThreadPoolExecutor schedulePool = new ScheduledThreadPoolExecutor(1);
        schedulePool.schedule(new Runnable() {
            @Override
            public void run() {
                System.out.println("你好-------------------");
            }
        }, 3, TimeUnit.SECONDS);

        schedulePool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                System.out.println("xxxxxx  ... ... ");
            }
        }, 2, 2, TimeUnit.SECONDS);

        Thread.sleep(10000000000L);
    }
//
//
//    /**
//     * 加入到延时队列中
//     *
//     * @param task
//     */
//    public void put(SessionManager.SessionCommitTask task) {
//        log.info("加入延时任务 task = ：{}", task);
//        delayQueue.put(task);
//    }
//
//    @Override
//    public void run(String... args) throws Exception {
////        log.info("初始化延时队列");
//        new Thread(() -> {
//            while (true) {
//                SessionManager.SessionCommitTask task = null;
//                try {
//                    task = delayQueue.take();
//                    if (task == null) {
//                        Thread.sleep(100);
//                    } else {
//                        sessionThreadPool.execute(task);
//                    }
//                } catch (InterruptedException e) {
//                    log.error("delay task  run 出错 , task = {} , e = {}", task, e);
//                }
//            }
//
//        }).start();
//    }


}
