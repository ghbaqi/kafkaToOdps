package com.hupu.hermes.sink.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.utils.StringUtils;
import com.hupu.hermes.sink.kafka.OdpsKafkaListener;
import com.hupu.hermes.sink.kafka.SpringKafkaListener;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//@ConditionalOnBean(Configuration.class)
@Component
@Slf4j
public class SessionManager {


    @Autowired
    private ScheduledThreadPoolExecutor sessionCommitThread;


    @Autowired
    private Configuration configuration;

    private TableSchema schema = null;
    private TableTunnel tunnel = null;
    private Table table = null;

    private Lock sessionLock = null;

//    private Lock sessionLock = null;


    /**
     * key   eg:et20191210actclick
     */
    private Map<String, SessionDecorator> sessionMap = new ConcurrentHashMap<>();

    private Map<String, WriterDecorator> bufferedWriterMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, Acknowledgment> acknowledgmentMap = new ConcurrentHashMap<>(128);

//    private static SessionManager singleton;

    /**
     * 记录 writer 上一次写入时间 , 生命周期只有 115 s
     */
    private Map<Integer, Long> writerTimeMap = new ConcurrentHashMap<>();


    private SessionManager() {
//        init();
    }

//    public static SessionManager getInstance() {
//        if (singleton == null) {
//            synchronized (SessionManager.class) {
//                singleton = new SessionManager();
//            }
//        }
//        return singleton;
//    }

    /**
     * 获取 session , 管理 session 的生命周期
     * <p>
     * partitionFieldMap 分区 , eg :  key et  --> value 20201123
     */
    public SessionDecorator getSession(Map<String, String> partitionFieldMap) throws OdpsException {

        sessionLock.lock();
        try {
            String partitionInfoStr = mkPartitionInfoStr(partitionFieldMap);
            if (sessionMap.get(partitionInfoStr) != null) {       //  sb  : et20191123actclick
                return sessionMap.get(partitionInfoStr);
            }

            PartitionSpec partition = new PartitionSpec();
            // 踩坑 分区需要有按照分区顺序创建
//        partition.set("ds", "20000511");
//        partition.set("act", "click");
            for (String partitionFiled : configuration.getPartitionFileds()) {
                if (StringUtils.isBlank(partitionFieldMap.get(partitionFiled))) {
                    throw new RuntimeException("未传入分区信息 , partitionFiled = " + partitionFiled);
                }
                partition.set(partitionFiled, partitionFieldMap.get(partitionFiled));
            }
            // 判断表是否有这个分区, 没有的话 ,需要先创建
            if (!table.hasPartition(partition)) {
                table.createPartition(partition);
            }

            TableTunnel.UploadSession session = tunnel.createUploadSession(configuration.getProjectName(), configuration.getTableName(), partition);
            SessionDecorator sessionDecorator = new SessionDecorator(session, partitionInfoStr);
            sessionMap.put(partitionInfoStr, sessionDecorator);
            log.info("创建了一个 session = " + sessionDecorator);


            /**
             * 同一个分区的数据时 , 前一个 session 的生命到期 , 需要移除并提交 ,
             * 然后再创建一个新的 session
             */
            // 一个 session 一段时间后需要 commit
            SessionCommitTask commitTask = new SessionCommitTask(sessionDecorator, partitionInfoStr);
            sessionCommitThread.schedule(commitTask, configuration.getSessionLiveTime(), TimeUnit.MINUTES);

            return sessionDecorator;
        } finally {
            sessionLock.unlock();
        }


    }

    /**
     * 分区 需要有序
     *
     * @param partitionFieldMap
     * @return
     */
    private String mkPartitionInfoStr(Map<String, String> partitionFieldMap) {
        StringBuffer sb = new StringBuffer();
        for (String partitionFiled : configuration.getPartitionFileds()) {
            if (StringUtils.isBlank(partitionFieldMap.get(partitionFiled))) {
                throw new RuntimeException("未传入分区信息 , partitionFiled = " + partitionFiled);
            }
            sb.append(partitionFiled);
            sb.append(partitionFieldMap.get(partitionFiled));
        }
        return sb.toString();
    }

    public WriterDecorator getBufferWriter(Map<String, String> partitionFieldMap) throws
            OdpsException, IOException {

        sessionLock.lock();   // TODO  为啥这里必须要加锁
        try {
            String partitionInfoStr = mkPartitionInfoStr(partitionFieldMap);

            WriterDecorator exitWriter = bufferedWriterMap.get(partitionInfoStr);
            if (exitWriter != null) {
                // 没有过期
                Long writerTime = writerTimeMap.get(exitWriter.hashCode());
                if (writerTime != null && System.currentTimeMillis() - writerTime < 115 * 1000) {    // NullPointerException: null
//                    System.out.println("获取已经存在 未过期的 writer");
                    writerTimeMap.put(exitWriter.hashCode(), System.currentTimeMillis());   // 更新本次的写入时间
                    return exitWriter;
                } else {
                    // 过期
                    log.info("exit writer 大于 115 s 没有数据写入 , 进行 close");
                    writerTimeMap.remove(exitWriter.hashCode());
                    bufferedWriterMap.remove(partitionInfoStr);
//                    System.out.println("writer 过期 , 重新创建");
                    if (exitWriter != null) {
                        exitWriter.close();
                    }
                }
            }

            SessionDecorator session = getSession(partitionFieldMap);
//            TunnelBufferedWriter writer = (TunnelBufferedWriter) session.openBufferedWriter();
            WriterDecorator writer = session.openWriter();
            bufferedWriterMap.put(partitionInfoStr, writer);
            writerTimeMap.put(writer.hashCode(), System.currentTimeMillis());
            return writer;
        } finally {
            sessionLock.unlock();
        }
    }


    @PostConstruct
    private void init() {
//        configuration = Configuration.getInstance();
        AliyunAccount account = new AliyunAccount(configuration.getAccessId(), configuration.getAccessKey());
        Odps odps = new Odps(account);
        odps.setEndpoint(configuration.getOdpsUrl());
        odps.setDefaultProject(configuration.getProjectName());
        table = odps.tables().get(configuration.getTableName());
        if (table == null) {
            throw new RuntimeException(String.format("odps 表不存在 , project = %s , table = %s", configuration.getProjectName(), configuration.getTableName()));
        }
        schema = table.getSchema();
        tunnel = new TableTunnel(odps);

        sessionLock = new ReentrantLock();
    }

    public TableSchema getTableSchema() {
        return schema;
    }


    /**
     * @param key = group+topic+partition
     * @param key = topic+partition
     */
    public void putAck(String key, Acknowledgment ack) {
        acknowledgmentMap.put(key, ack);
    }


    @ToString
    public class SessionCommitTask implements Runnable {


        private SessionDecorator sessionDecorator;
        private String partitionInfoStr;

        public SessionCommitTask(SessionDecorator session, String partitionInfoStr) {
            this.sessionDecorator = session;
            this.partitionInfoStr = partitionInfoStr;
        }

        @Override
        public void run() {
            sessionLock.lock();
            try {
//                springKafkaListener.commit();
                WriterDecorator bufferedWriter = bufferedWriterMap.get(partitionInfoStr);
                if (bufferedWriter != null) {
                    bufferedWriterMap.remove(partitionInfoStr);
                    writerTimeMap.remove(bufferedWriter.hashCode());
                }
                sessionMap.remove(partitionInfoStr);
                sessionCommitThread.schedule(() -> {
                    try {
                        bufferedWriter.close();
                        sessionDecorator.commit();
                        for (String key : acknowledgmentMap.keySet()) {
                            acknowledgmentMap.get(key).acknowledge();
                        }
                        acknowledgmentMap.clear();

                        log.info("完成一次session提交&&完成offset提交 = " + sessionDecorator);
                    } catch (Exception e) {
                        log.error("writerClose | sessionCommit error = {}", e);
                    } finally {
                        //  TODO
//                bufferedWriterMap.remove(partitionInfoStr);
//                sessionMap.remove(partitionInfoStr);
                    }
                }, 0, TimeUnit.SECONDS);

            } finally {
                sessionLock.unlock();
            }
        }

    }


}

