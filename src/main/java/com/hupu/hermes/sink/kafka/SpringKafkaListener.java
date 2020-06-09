package com.hupu.hermes.sink.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


@Slf4j
@Component
public class SpringKafkaListener {

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

    private SessionDecorator recordSession;


    //    @KafkaListener(topics = "${kafka.odps.consumer.topic}", concurrency = "${kafka.odps.consumer.threads}", properties = "max.poll.interval.ms=600000")
    @KafkaListener(topics = {"user_track", "user_track_20191031"}, concurrency = "${kafka.odps.consumer.threads}", properties = "max.poll.interval.ms=600000")
//    @KafkaListener(topics = {"0318"}, concurrency = "${kafka.odps.consumer.threads}", properties = "max.poll.interval.ms=600000")
    public void listen(ConsumerRecord<String, String> cr, Acknowledgment ack) {

        String value = cr.value();
        JSONObject jsonObject;
        try {
            jsonObject = JSON.parseObject(value);
        } catch (Exception e) {
            log.error("解析数据错误1 msg =  {} , exception = {}", value, e);
            return;
        }

        // 通过 分区信息 拿 session
        Long etLong = jsonObject.getLong("et");
        Long serverTime = jsonObject.getLong("server_time");
        if (serverTime == null || serverTime < 0) {
            serverTime = System.currentTimeMillis();
        }
        // 丢弃脏数据
        if (etLong == null) {
            log.error("没有et字段2 , {}", jsonObject);
            return;
        }
        if (System.currentTimeMillis() - etLong > 96 * 60 * 60 * 1000) {
            log.error("4天前的数据3 , {}", jsonObject);
            return;
        }
        if (etLong > serverTime) {
            jsonObject.put("et", serverTime);
        }


        //  key = group+topic+partition
        //  key = topic+partition  ,  map 中最多会存放 120 个 ack
        sessionManager.putAck(cr.topic() + cr.partition(), ack);


        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
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

            if (recordSession == null) {
                recordSession = sessionManager.getSession(partitionFieldMap);
            }
            if (bufferWriter == null)
                bufferWriter = sessionManager.getBufferWriter(partitionFieldMap);
            writeRecord(bufferWriter, jsonObject, recordSession.newRecord());

        } catch (IOException | OdpsException e) {
            log.error("写入 event 失败 , msg = " + value + " , ex = " + e);
        }
    }


    /**
     * "meta":{
     * "pos":"9912611",
     * "line_number":"3536",
     * "file_name":"/data/hermes/access_log/7/access_log.2020033100",
     * "host":"10.31.0.168"
     * }
     */
    private void writeRecord(WriterDecorator bufferWriter, JSONObject jsonObject, Record record) throws IOException {
        TableSchema schema = sessionManager.getTableSchema();
        Column unknownColumn = schema.getColumn("unknown");
        for (int i = 0; i < schema.getColumns().size(); i++) {
            Column column = schema.getColumn(i);

            if (column.getName().contains("meta"))
                continue;


            // 对于 ab  和  themis_ab 特殊处理
            if (column.getName().equals("ab") || column.getName().equals("themis_ab") || column.getName().equals("themis_ab_json")) {
                String ab = jsonObject.getString("ab");
                String themis = jsonObject.getString("themis_ab");

                if (ab != null && !ab.trim().equals("") && column.getName().equals("ab")) {
                    record.setString(i, getAbValue(ab));
                }

                if (themis != null && !themis.trim().equals("") && column.getName().equals("themis_ab")) {
                    record.setString(i, getThemisAb(themis));
                }

                if (themis != null && !themis.trim().equals("") && column.getName().equals("themis_ab_json")) {
                    record.setString(i, getThemisAbJson(themis));
                }

//                jsonObject.remove(column.getName());

                continue;
            }


            switch (column.getType()) {
                case BIGINT:
                    record.setBigint(i, jsonObject.getLong(column.getName()));
                    jsonObject.remove(column.getName());
                    break;
                case BOOLEAN:
                    record.setBoolean(i, jsonObject.getBoolean(column.getName()));
                    jsonObject.remove(column.getName());
                    break;
                case DATETIME:
                    record.setDatetime(i, jsonObject.getDate(column.getName()));
                    jsonObject.remove(column.getName());
                    break;
                case DOUBLE:
                    record.setDouble(i, jsonObject.getDouble(column.getName()));
                    jsonObject.remove(column.getName());
                    break;
                case STRING:
                    record.setString(i, jsonObject.getString(column.getName()));
                    jsonObject.remove(column.getName());
                    break;
                default:
                    throw new RuntimeException("Unknown column type: " + column.getType());
            }
        }

        jsonObject.remove("ab");
        jsonObject.remove("themis_ab");

        JSONObject meta = jsonObject.getObject("meta", JSONObject.class);
        if (meta != null) {
            record.setBigint("meta_pos", meta.getLong("pos"));
            record.setBigint("meta_line_number", meta.getLong("line_number"));
            record.setString("meta_file_name", meta.getString("file_name"));
            record.setString("meta_host", meta.getString("host"));
            jsonObject.remove("meta");
        }
        jsonObject.remove("act");
        if (unknownColumn != null) {
            record.setString("unknown", jsonObject.toJSONString());
        }

//        Caused by: java.lang.NullPointerException: null  [2020-03-31 15:59:27.027]
//        at com.hupu.hermes.sink.kafka.SpringKafkaListener.writeRecord(SpringKafkaListener.java:163)
        bufferWriter.write(record);
    }

    private String getAbValue(String s) {

        try {
            JSONObject ab = JSON.parseObject(s);
            Map<String, String> mapRes = new HashMap<>();
            for (String obj : ab.keySet()) {
                Object o = ab.get(obj);
                if (o != null) {
                    JSONObject json = JSON.parseObject(o.toString());
                    mapRes.put(obj, json.getString("val"));
                }
            }
            return mapRes.toString().substring(1, mapRes.toString().length() - 1);
        } catch (Exception e) {
            log.error("解析 ab 错误 e = " + e);
        }
        return "";

    }


    private String getThemisAbJson(String s) {

        try {
            JSONArray themisArr = JSON.parseArray(s);
            if (themisArr.size() == 0)
                return "";

            JSONObject res = new JSONObject();
            for (int i = 0; i < themisArr.size(); i++) {
                Object obj = themisArr.get(i);
                JSONObject json = JSON.parseObject(obj.toString());

                res.put(json.getString("k"), json.getString("v"));
            }
            String resStr = res.toJSONString();
            if (resStr.equals("{}"))
                resStr = "";
            return resStr;
        } catch (Exception e) {
            log.error("解析 themesabJson 错误 e = " + e);
        }

        return "";

    }


    private String getThemisAb(String s) {

        try {
            JSONArray themisArr = JSON.parseArray(s);
            Map<String, String> map = new HashMap<>();
            for (int i = 0; i < themisArr.size(); i++) {
                Object obj = themisArr.get(i);
                JSONObject json = JSON.parseObject(obj.toString());
                map.put(json.getString("k"), json.getString("v"));
            }
            return map.toString().substring(1, map.toString().length() - 1);
        } catch (Exception e) {
            log.error("解析 themesab 错误 e = " + e);
        }

        return "";

    }

}
