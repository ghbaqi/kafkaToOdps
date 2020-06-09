package com.hupu.hermes.sink.odps;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.CompressOption;
import lombok.ToString;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class SessionDecorator {


    private TableTunnel.UploadSession session;



    private String partitionInfoStr;

//    public SessionDecorator(TableTunnel.UploadSession session) {
//        this.session = session;
//        kafkaConsumers = new CopyOnWriteArraySet<>();
//    }

    public SessionDecorator(TableTunnel.UploadSession session, String partitionInfoStr) {
        this.session = session;

        this.partitionInfoStr = partitionInfoStr;
    }


    public WriterDecorator openWriter() throws IOException {
        CompressOption compressOption = new CompressOption();
        WriterDecorator writerDecorator = new WriterDecorator(session, compressOption);
        return writerDecorator;

    }

    public void commit() throws TunnelException, IOException {
        session.commit();
    }

    public Record newRecord() {
        return session.newRecord();
    }

    @Override
    public String toString() {
        return "Session:" + partitionInfoStr;
    }
}
