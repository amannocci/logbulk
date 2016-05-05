/**
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.*;

/**
 * This class is able to convert an IO stream to a NIO stream.
 */
public class AsyncInputStream implements ReadStream<Buffer> {

    // Some constants
    public static final int STATUS_PAUSED = 0, STATUS_ACTIVE = 1, STATUS_CLOSED = 2;
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
    public AsyncInputStream(Vertx vertx, ExecutorService executor, InputStream in, int chunkSize) {
        checkNotNull(in, "The input stream can't be null");
        checkNotNull(vertx, "The vertx can't be null");
        checkArgument(chunkSize > 0, "chunkSize: " + chunkSize + " (expected: a positive integer)");
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
        checkNotNull(handler, "The handler can't be null");
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
            final Handler<Buffer> dataHandler = this.dataHandler;
            final Handler<Void> closeHandler = this.closeHandler;
            executor.execute(() -> {
                try {
                    final byte[] bytes = readChunk();
                    if (bytes.length == 0) {
                        status = STATUS_CLOSED;
                        vertx.runOnContext(event -> {
                            if (closeHandler != null) {
                                closeHandler.handle(null);
                            }
                        });
                    } else {
                        vertx.runOnContext(event -> {
                            dataHandler.handle(Buffer.buffer(bytes));
                            doRead();
                        });
                    }
                } catch (final Exception e) {
                    status = STATUS_CLOSED;
                    IOUtils.closeQuietly(in);
                    vertx.runOnContext(event -> {
                        if (failureHandler != null) {
                            failureHandler.handle(e);
                        }
                    });
                }
            });
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
     * @throws Exception something wrong.
     */
    public boolean isEndOfInput() throws Exception {
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
     * @throws Exception something is wrong.
     */
    private byte[] readChunk() throws Exception {
        // End of stream
        if (isEndOfInput()) return ArrayUtils.EMPTY_BYTE_ARRAY;

        // Number of readable bytes
        final int availableBytes = in.available();

        // Chunk size
        final int chunkSize;
        if (availableBytes <= 0) {
            chunkSize = this.chunkSize;
        } else {
            chunkSize = Math.min(this.chunkSize, in.available());
        }

        // Some buffer stuff
        byte[] buffer;
        try {
            // Transfer to buffer
            byte[] tmp = new byte[chunkSize];
            int readBytes = in.read(tmp);
            if (readBytes <= 0) {
                return ArrayUtils.EMPTY_BYTE_ARRAY;
            }
            buffer = tmp;
            offset += tmp.length;
            return buffer;
        } catch (IOException e) {
            IOUtils.closeQuietly(in);
            return ArrayUtils.EMPTY_BYTE_ARRAY;
        }
    }

}