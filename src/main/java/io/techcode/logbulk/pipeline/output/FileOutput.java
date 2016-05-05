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
package io.techcode.logbulk.pipeline.output;

import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.component.Mailbox;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;

/**
 * File output pipeline component.
 */
@Slf4j
public class FileOutput extends ComponentVerticle {

    // Async file instance
    private AsyncFile file;

    // Use a sub buffer
    private Buffer buf;

    @Override public void start() {
        // Configuration
        JsonObject config = config();

        // Setup processing task
        String path = config.getString("path");
        String delimiter = config.getString("delimiter", "\n");
        String endpoint = config.getString("endpoint");
        String endpointUuid = endpoint + '.' + getUuid();
        int chunk = config.getInteger("chunk", 8192);
        int chunkPartition = chunk / 4;
        file = vertx.fileSystem().openBlocking(path, new OpenOptions().setWrite(true).setCreate(true));
        file.setWriteQueueMaxSize(chunk);
        buf = Buffer.buffer(chunkPartition);

        // Register endpoint
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(endpoint)
                .handler(new Mailbox(this, endpoint, config.getInteger("mailbox", Mailbox.DEFAULT_THREEHOLD), evt -> {
                    // Process the event
                    buf.appendString(mask(evt).encode()).appendString(delimiter);
                    if (buf.length() > chunkPartition) {
                        file.write(buf);
                        buf = Buffer.buffer(chunkPartition);
                        if (file.writeQueueFull()) {
                            pause(source(evt), endpointUuid);
                            file.drainHandler(h -> resume(source(evt), endpointUuid));
                        }
                    }

                    // Send to the next endpoint
                    forward(evt);
                }));
    }

    @Override public void stop() {
        if (buf.length() > 0) file.write(buf);
        file.flush();
        file.close();
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("endpoint") != null, "The endpoint is required");
        checkState(config.getString("path") != null, "The path is required");
        return config;
    }

}
