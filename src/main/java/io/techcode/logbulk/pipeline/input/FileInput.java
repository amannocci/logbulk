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
package io.techcode.logbulk.pipeline.input;

import io.techcode.logbulk.component.ComponentVerticle;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;

/**
 * File input pipeline component.
 */
@Slf4j
public class FileInput extends ComponentVerticle {

    // Record parser for delimitation
    private RecordParser parser;

    // Async file instance
    private AsyncFile file;

    @Override public void start() {
        super.start();

        // Configuration
        parser = inputParser(config);

        // Setup processing task
        String path = config.getString("path");
        int chunk = config.getInteger("chunk", 8192);
        file = vertx.fileSystem().openBlocking(path, new OpenOptions().setRead(true));
        file.setReadBufferSize(chunk);

        // Handle back-pressure
        handlePressure(file, config);

        // Begin to read
        file.handler(buf -> parser.handle(buf));
        file.exceptionHandler(h -> {
            log.error("Error during file read:", h.getCause());
            vertx.close();
        });
    }

    @Override public void stop() {
        file.close();
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("path") != null, "The path is required");
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        return config;
    }

}