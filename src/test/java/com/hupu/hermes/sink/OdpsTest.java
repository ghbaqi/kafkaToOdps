package com.hupu.hermes.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.hupu.hermes.sink.odps.Configuration;
import com.hupu.hermes.sink.odps.SessionManager;
import com.hupu.hermes.sink.odps.WriterDecorator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

@RunWith(SpringRunner.class)
@SpringBootTest
public class OdpsTest {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private Configuration configuration;

    /**
     * 使用自己的 sessionManager 上传 , 有多出数据的  bug
     */
    @Test
    public void test01() throws OdpsException, IOException {

        UserTrack bean = new UserTrack();
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

        Record record = null;


        /**
         * 2000 万条数据, 传输耗时
         * total time minute = 1.4811833333333333
         * total time minute = 1.3878666666666666
         * total time minute = 1.8408333333333333
         */
        long start = System.currentTimeMillis();
        for (long i = 1; i <= 2000 * 10000L; i++) {
            bean.setEt(System.currentTimeMillis());
            bean.setDur(i);
//            bean.setAct("x1");
            if (i % 5 == 0) {
                bean.setAct("a0");
            } else if (i % 5 == 1) {
                bean.setAct("a1");
            } else if (i % 5 == 2) {
                bean.setAct("a2");
            } else if (i % 5 == 3) {
                bean.setAct("a3");
            } else {
                bean.setAct("a4");
            }
            JSONObject jsonObject = JSON.parseObject(JSON.toJSONString(bean));

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            String et = simpleDateFormat.format(new Date(jsonObject.getLong("et")));

            HashMap<String, String> partitionFieldMap = new HashMap<>();

            partitionFieldMap.put("ds", et); // 时间分区
            //   partitionFieldMap.put("ds", "0302-2"); // 时间分区

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

            WriterDecorator bufferWriter = sessionManager.getBufferWriter(partitionFieldMap);

            if (record == null)
                record = sessionManager.getSession(partitionFieldMap).newRecord();
            writeRecord(bufferWriter, jsonObject, record);

        }

        double minute = (System.currentTimeMillis() - start) / 60000.0;
        System.out.println("total time minute = " + minute);
        try {
            Thread.sleep(999999L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
//        LOG.info("写入一条数据成功 , event = " + jsonObject.toJSONString());
//        System.out.println("写入一条数据成功 dur = " + jsonObject.getString("dur"));


    }


}
