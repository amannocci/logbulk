/*
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
package io.techcode.logbulk.pipeline.output;

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.vertx.core.VoidHandler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkState;

/**
 * File output pipeline component.
 */
public class FileOutput extends BaseComponentVerticle {

    // Async file instance
    private AsyncFile file;

    // Use a sub buffer
    private Buffer buf;

    // Settings
    private String delimiter;
    private int chunkPartition;

    // Handle pressure
    private final VoidHandler HANDLE_PRESSURE = new VoidHandler() {
        @Override protected void handle() {
            release();
        }
    };

    @Override public void start() {
        super.start();

        // Setup processing task
        String path = config.getString("path");
        delimiter = config.getString("delimiter", "\n");
        int chunk = config.getInteger("chunk", 8192);
        chunkPartition = chunk / 4;
        file = vertx.fileSystem().openBlocking(path, new OpenOptions().setWrite(true).setCreate(true));
        file.setWriteQueueMaxSize(chunk);
        buf = Buffer.buffer(chunkPartition);
    }

    @Override public void handle(JsonObject msg) {
        // Process the body
        buf.appendString(body(msg).encode()).appendString(delimiter);
        if (buf.length() > chunkPartition) {
            file.write(buf);
            buf = Buffer.buffer(chunkPartition);
            if (file.writeQueueFull()) {
                file.drainHandler(HANDLE_PRESSURE);
                forward(msg);
                return;
            }
        }
        forwardAndRelease(msg);
    }

    @Override public void stop() {
        if (buf.length() > 0) file.write(buf);
        file.flush();
        file.close();
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("path") != null, "The path is required");
    }

}
