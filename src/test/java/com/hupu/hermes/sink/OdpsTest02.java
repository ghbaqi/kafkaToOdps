package com.hupu.hermes.sink;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class OdpsTest02 {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private Configuration configuration;

    private SessionDecorator recordSession;

    /**
     *
     */
    @Test
    public void test01() throws OdpsException, IOException, InterruptedException {

//        final long TATAL = 10000 * 10000L;
        final long TATAL = 10000*10000L;

        ExecutorService threadPool = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(5);
        long start = System.currentTimeMillis();
        for (int j = 0; j < 5; j++) {
            int finalJ = j;
            threadPool.submit(() -> {
                UserTrack bean = new UserTrack();
                for (long i = 1; i <= TATAL / 5; i++) {
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

                    WriterDecorator bufferWriter = null;
                    try {
                        bufferWriter = sessionManager.getBufferWriter(partitionFieldMap);

                        if (recordSession == null) {
                            recordSession = sessionManager.getSession(partitionFieldMap);
                        }

                        writeRecord(bufferWriter, jsonObject, recordSession.newRecord());
                    } catch (OdpsException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }
                System.out.println("一个线程完成 j = " + finalJ);
                latch.countDown();
            });
        }


        latch.await();
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
