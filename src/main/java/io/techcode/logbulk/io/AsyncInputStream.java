/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.techcode.logbulk.io;

import com.google.common.util.concurrent.MoreExecutors;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class is able to convert an IO stream to a NIO stream.
 */
public class AsyncInputStream implements ReadStream<Buffer> {

    // Some constants
    public static final int STATUS_PAUSED = 0;
    public static final int STATUS_ACTIVE = 1;
    public static final int STATUS_CLOSED = 2;
    static final int DEFAULT_CHUNK_SIZE = 8192;

    // Some stuff
    private final Vertx vertx;
    private final Executor executor;
    private final PushbackInputStream in;
    private final int chunkSize;

    // Status
    @Getter private int status = STATUS_ACTIVE;

    // All handlers
    private Handler<Void> closeHandler;
    private Handler<Buffer> dataHandler;
    private Handler<Throwable> failureHandler;

    // Offset
    private int offset;

    /**
     * Create a new async input stream.
     *
     * @param vertx    vertx instance.
     * @param executor blocking executor.
     * @param in       IO input stream.
     */
    public AsyncInputStream(Vertx vertx, ExecutorService executor, InputStream in) {
        this(vertx, executor, in, DEFAULT_CHUNK_SIZE);
    }

    /**
     * Create a new async input stream.
     *
     * @param vertx     vertx instance.
     * @param executor  blocking executor.
     * @param in        IO input stream.
     * @param chunkSize chunk size.
     */
    public AsyncInputStream(@NonNull Vertx vertx, ExecutorService executor, @NonNull InputStream in, int chunkSize) {
        checkArgument(chunkSize > 0, "chunkSize: %d (expected: a positive integer)", chunkSize);
        this.vertx = vertx;
        if (in instanceof PushbackInputStream) {
            this.in = (PushbackInputStream) in;
        } else {
            this.in = new PushbackInputStream(in);
        }
        this.chunkSize = chunkSize;
        MoreExecutors.addDelayedShutdownHook(executor, 250, TimeUnit.MILLISECONDS);
        this.executor = executor;
    }

    @Override public AsyncInputStream endHandler(Handler<Void> endHandler) {
        closeHandler = endHandler;
        return this;
    }

    @Override public AsyncInputStream handler(Handler<Buffer> handler) {
        dataHandler = handler;
        doRead();
        return this;
    }

    @Override public AsyncInputStream pause() {
        if (status == STATUS_ACTIVE) {
            status = STATUS_PAUSED;
        }
        return this;
    }

    @Override public AsyncInputStream resume() {
        checkState(status != STATUS_CLOSED);
        if (status == STATUS_PAUSED) {
            status = STATUS_ACTIVE;
            doRead();
        }
        return this;
    }

    @Override public AsyncInputStream exceptionHandler(Handler<Throwable> handler) {
        failureHandler = handler;
        return this;
    }

    /**
     * Do a read cycle.
     */
    private void doRead() {
        if (status == STATUS_ACTIVE) {
            executor.execute(() -> {
                try {
                    final byte[] bytes = readChunk();
                    if (bytes.length == 0) {
                        status = STATUS_CLOSED;
                        vertx.runOnContext(e -> fireClose());
                    } else {
                        vertx.runOnContext(e -> {
                            fireData(Buffer.buffer(bytes));
                            doRead();
                        });
                    }
                } catch (final Exception ex) {
                    status = STATUS_CLOSED;
                    closeQuietly(in);
                    vertx.runOnContext(e -> fireException(ex));
                }
            });
        }
    }

    /**
     * Fire a close event if handler defined.
     */
    private void fireClose() {
        if (closeHandler != null) {
            closeHandler.handle(null);
        }
    }

    /**
     * Fire a data event if handler defined.
     *
     * @param buf data involved.
     */
    private void fireData(Buffer buf) {
        if (dataHandler != null) {
            dataHandler.handle(buf);
        }
    }

    /**
     * Fire an exception event if handler defined.
     *
     * @param ex exception involved.
     */
    private void fireException(Exception ex) {
        if (failureHandler != null) {
            failureHandler.handle(ex);
        }
    }

    /**
     * Returns the number of bytes transferred.
     *
     * @return number of bytes transferred.
     */
    public long transferredBytes() {
        return offset;
    }

    /**
     * Returns true if the stream is closed.
     *
     * @return true if the stream is closed.
     */
    public boolean isClosed() {
        return status == STATUS_CLOSED;
    }

    /**
     * Returns true if the end of stream has been reach.
     *
     * @return true if the end of stream has been reach.
     * @throws IOException something wrong.
     */
    public boolean isEndOfInput() throws IOException {
        int b = in.read();
        if (b < 0) {
            return true;
        } else {
            in.unread(b);
            return false;
        }
    }

    /**
     * Attempt to read a full chunk.
     *
     * @return chunk readed.
     * @throws IOException something is wrong.
     */
    private byte[] readChunk() throws IOException {
        // End of stream
        if (isEndOfInput()) {
            return ArrayUtils.EMPTY_BYTE_ARRAY;
        }

        // Number of readable bytes
        final int availableBytes = in.available();

        // Partition size
        final int partitionSize;
        if (availableBytes <= 0) {
            partitionSize = this.chunkSize;
        } else {
            partitionSize = Math.min(this.chunkSize, availableBytes);
        }

        // Some buffer stuff
        byte[] buffer;
        try {
            // Transfer to buffer
            byte[] tmp = new byte[partitionSize];
            int readBytes = in.read(tmp);
            if (readBytes <= 0) {
                return ArrayUtils.EMPTY_BYTE_ARRAY;
            }
            buffer = tmp;
            offset += tmp.length;
            return buffer;
        } catch (IOException ignored) {
            closeQuietly(in);
            return ArrayUtils.EMPTY_BYTE_ARRAY;
        }
    }

    /**
     * Closes a Closeable unconditionally.
     *
     * @param closeable the object to close, may be null or already closed.
     */
    private void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException ignored) {
        }
    }

}