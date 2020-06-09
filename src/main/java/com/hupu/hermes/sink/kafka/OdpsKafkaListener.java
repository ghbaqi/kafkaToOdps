package com.hupu.hermes.sink.kafka;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.hupu.hermes.sink.odps.Configuration;
import com.hupu.hermes.sink.odps.SessionDecorator;
import com.hupu.hermes.sink.odps.SessionManager;
import com.hupu.hermes.sink.odps.WriterDecorator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
//@Component
public class OdpsKafkaListener {

    @Autowired
    private Executor kafkaThreadPool;

//    @Autowired
//    private Executor dealMsgThreadPool;


    @Autowired
    private Configuration configuration;

    @Autowired
    private SessionManager sessionManager;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaUrl;

    @Value("${kafka.odps.consumer.topic}")
    private String topic;

    @Value("${spring.kafka.consumer.group-id}")
    private String group;

    @Value("${kafka.odps.consumer.threads}")
    private int threads;

    private AtomicBoolean commitFlag = new AtomicBoolean(false);


    @PostConstruct
    public void init() {
        Properties props = new Properties();
        //Kafka 集群
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaUrl);
        //消费者组，只要 group.id 相同，就属于同一个消费者组 props.put("group.id", "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//关闭自动提交 offset
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        // 500 是默认值
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 250);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300 * 1000 * 3);

//
        for (int i = 0; i < threads; i++) {
            kafkaThreadPool.execute(() -> {

//                Thread thread = Thread.currentThread();

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
                Record record = null;
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
                consumer.subscribe(Arrays.asList(topic));
                log.info("开启一个 kafka consumer");
                while (true) {

                    // 为什么还有一些数据没有提交 offset  ? ?  导致了重复数据  ; 经再次测试 , 已修复
                    if (commitFlag.get()) {
                        consumer.commitAsync((map, e) -> {
                            if (e == null) {
                                log.info("完成一次kafkaOffset提交");
                                commitFlag.set(false);
                            } else {
                                log.error("kafkaOffset提交异常 e = " + e);
                            }
                        });
                    }

                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
//                    dealMsgThreadPool.execute(new DealMsgTask(records, consumer));
                    for (ConsumerRecord<String, String> cr : records) {
                        String value = cr.value();
//                        log.info(value);
                        JSONObject jsonObject = JSON.parseObject(value);
                        //  log.info("threadName = {} , act = {} , dur = {}", Thread.currentThread().getName(), jsonObject.get("act"), jsonObject.get("dur"));


                        // 通过 分区信息 拿 session

                        Long etLong = jsonObject.getLong("et");
                        // 丢弃脏数据
                        if (etLong == null || System.currentTimeMillis() - etLong > 3 * 24 * 60 * 60 * 1000 || etLong - System.currentTimeMillis() > 1 * 60 * 60 * 1000) {
                            continue;
                        }

                        String et = simpleDateFormat.format(new Date(jsonObject.getLong("et")));
                        HashMap<String, String> partitionFieldMap = new HashMap<>();
                        partitionFieldMap.put("ds", et); // 时间分区
                        for (String f : configuration.getPartitionFileds()) {  // 其它分区列
                            if ("ds".equals(f))
                                continue;
                            partitionFieldMap.put(f, (String) jsonObject.get(f));
                        }


                        /**
                         *
                         * 这里不同的 event 进来会属于不同的分区 , 不同的分区拿到不同的 session
                         * 同一个 session 需要只打开一个 recordWriter
                         * 分区 --> session  -> blockId  ->  recordWriter
                         */
                        WriterDecorator bufferWriter = null;
                        try {
                            bufferWriter = sessionManager.getBufferWriter(partitionFieldMap);
//                            SessionDecorator session = sessionManager.getSession(partitionFieldMap);
//                            writeRecord(bufferWriter, jsonObject, session);
                            if (record == null)
                                record = sessionManager.getSession(partitionFieldMap).newRecord();
                            writeRecord(bufferWriter, jsonObject, record);

                        } catch (IOException | OdpsException e) {
                            log.error("写入 event 失败 , msg = " + value + " , ex = " + e);
                        }

                    }

                } ///

            });
        }

    }

    public void commit() {
        commitFlag.set(true);
    }


    private void writeRecord(WriterDecorator bufferWriter, JSONObject jsonObject, Record record) throws IOException {
        TableSchema schema = sessionManager.getTableSchema();
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);
            switch (column.getType()) {
                case BIGINT:
                    record.setBigint(i, jsonObject.getLong(column.getName()));
                    break;
                case BOOLEAN:
                    record.setBoolean(i, jsonObject.getBoolean(column.getName()));
                    break;
                case DATETIME:
                    record.setDatetime(i, jsonObject.getDate(column.getName()));
                    break;
                case DOUBLE:
                    record.setDouble(i, jsonObject.getDouble(column.getName()));
                    break;
                case STRING:
                    record.setString(i, jsonObject.getString(column.getName()));
                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + column.getType());
            }
        }
        bufferWriter.write(record);
    }


    private void writeRecord(WriterDecorator bufferWriter, JSONObject jsonObject, SessionDecorator session) throws IOException {
//        LOG.info("writeRecord");
        Record record = session.newRecord();
        for (int i = 0; i < sessionManager.getTableSchema().getColumns().size(); i++) {
            Column column = sessionManager.getTableSchema().getColumn(i);
            switch (column.getType()) {
                case BIGINT:
                    record.setBigint(i, jsonObject.getLong(column.getName()));
                    break;
                case BOOLEAN:
                    record.setBoolean(i, jsonObject.getBoolean(column.getName()));
                    break;
                case DATETIME:
                    record.setDatetime(i, jsonObject.getDate(column.getName()));
                    break;
                case DOUBLE:
                    record.setDouble(i, jsonObject.getDouble(column.getName()));
                    break;
                case STRING:
                    record.setString(i, jsonObject.getString(column.getName()));
                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + column.getType());
            }
        }
        bufferWriter.write(record);
    }

}
