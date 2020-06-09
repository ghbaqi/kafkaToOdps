package com.hupu.hermes.sink.odps;

import com.aliyun.odps.commons.util.RetryExceedLimitException;
import com.aliyun.odps.commons.util.RetryStrategy;
import com.aliyun.odps.commons.util.backoff.BackOffStrategy;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.aliyun.odps.tunnel.io.Checksum;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.tunnel.io.ProtobufRecordPack;
import com.aliyun.odps.tunnel.io.TunnelBufferedWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class WriterDecorator implements RecordWriter {


    private ProtobufRecordPack bufferedPack;
    private TableTunnel.UploadSession session;
    private RetryStrategy retry;
    private long bufferSize;
    private long bytesWritten;

    private static final long BUFFER_SIZE_DEFAULT = 128 * 1024 * 1024;
    private static final long BUFFER_SIZE_MIN = 1024 * 1024;
    private static final long BUFFER_SIZE_MAX = 1000 * 1024 * 1024;

    private Lock lock;

    /**
     * 构造此类对象，使用默认缓冲区大小为 10 MiB，和默认的回退策略：4s、8s、16s、32s、64s、128s
     *
     * @param session {@link  TableTunnel.UploadSession}
     * @param option  {@link CompressOption}
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public WriterDecorator(TableTunnel.UploadSession session, CompressOption option)
            throws IOException {
        this.bufferedPack = new ProtobufRecordPack(session.getSchema(), new Checksum(), option);
        this.session = session;
        this.bufferSize = BUFFER_SIZE_DEFAULT;
//        this.retry = new RetryStrategy(6, 4, RetryStrategy.BackoffStrategy.EXPONENTIAL_BACKOFF);
        this.retry = new TunnelRetryStrategy();
        this.bytesWritten = 0;
        lock = new ReentrantLock();
    }

    /**
     * 设置缓冲区大小
     *
     * @param bufferSize 缓冲区大小字节，可以设置的最小值 1 MiB，最大值为 1000 MiB
     */
    public void setBufferSize(long bufferSize) {
        if (bufferSize < BUFFER_SIZE_MIN) {
            throw new IllegalArgumentException("buffer size must >= " + BUFFER_SIZE_MIN
                    + ", now: " + bufferSize);
        }
        if (bufferSize > BUFFER_SIZE_MAX) {
            throw new IllegalArgumentException("buffer size must <= " + BUFFER_SIZE_MAX
                    + ", now: " + bufferSize);
        }
        this.bufferSize = bufferSize;
    }

    /**
     * 设置重试策略
     *
     * @param strategy {@link RetryStrategy}
     */
    public void setRetryStrategy(RetryStrategy strategy) {
        this.retry = strategy;
    }

    /**
     * 将 record 写入缓冲区，当其大小超过 bufferSize 时，上传缓冲区中的记录过程中如果发生错误将
     * 进行自动重试，这个过程中 write 调用将一直阻塞，直到所有记录上传成功为止。
     *
     * @param r {@link Record}对象
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void write(Record r) throws IOException {
        lock.lock();
        try {
            bufferedPack.append(r);
            if (bufferedPack.getTotalBytes() > bufferSize) {
                flush();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭这个 writer，并上传缓存中没有上传过的记录。
     *
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void close() throws IOException {
        lock.lock();
        try {
            flush();
        } finally {
            lock.unlock();
        }

    }

    /**
     * 获得总共写的字节数（记录序列化）
     *
     * @return
     */
    public long getTotalBytes() throws IOException {
        flush();
        return bytesWritten;
    }

    /**
     * 主要是将  flush 方法改成线程安全的
     */
    private void flush() throws IOException {
        // 得到实际序列化的的字节数，如果等于 0，说明没有写，跳过即可
//        log.info("flush");
        try {
            Class<ProtobufRecordPack> packClass = ProtobufRecordPack.class;
            Method method = packClass.getDeclaredMethod("getTotalBytesWritten");
            method.setAccessible(true);
            long delta = (long) method.invoke(bufferedPack);
            // 通过反射调用 protected 方法
//            long delta = bufferedPack.getTotalBytesWritten();

            if (delta > 0) {
                bytesWritten += delta;
                Long blockId = session.getAvailBlockId();
                while (true) {
                    try {
                        session.writeBlock(blockId, bufferedPack);
                        bufferedPack.reset();
                        return;
                    } catch (IOException e) {
                        try {
                            retry.onFailure(e);
                        } catch (RetryExceedLimitException ignore) {
                            throw e;
                        }
                    }
                }
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("ProtobufRecordPack 反射调用出错1 e = " + e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("ProtobufRecordPack 反射调用出错2 e = " + e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("ProtobufRecordPack 反射调用出错3 e = " + e);
        } finally {
        }


    }

    static class TunnelRetryStrategy extends RetryStrategy {
        private static final int limit = 6;
        private static final int interval = 4;

        TunnelRetryStrategy() {
            super(6, 4, BackoffStrategy.EXPONENTIAL_BACKOFF);
        }

        TunnelRetryStrategy(int limit, BackOffStrategy strategy) {
            super(limit, strategy);
        }

        protected boolean needRetry(Exception e) {
            TunnelException err = null;
            if (e.getCause() instanceof TunnelException) {
                err = (TunnelException) e.getCause();
            }

            if (e instanceof TunnelException) {
                err = (TunnelException) e;
            }

            return err == null || err.getStatus() == null || err.getStatus() / 100 != 4;
        }
    }

}
