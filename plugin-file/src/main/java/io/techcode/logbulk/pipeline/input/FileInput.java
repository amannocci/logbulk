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
package io.techcode.logbulk.pipeline.input;

import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.concurrent.VertxScheduler;
import io.vertx.core.Future;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * File input pipeline component.
 */
public class FileInput extends ComponentVerticle {

    // Async file instance
    private AsyncFile file;

    @Override public void start() {
        super.start();

        // Configuration
        int interval = config.getInteger("interval", 1);
        int intervalMax = config.getInteger("intervalMax", 60);
        int maxAttempts = config.getInteger("maxAttempts", -1);
        RetryPolicy retryPolicy = new RetryPolicy()
                .withBackoff(interval, intervalMax, TimeUnit.SECONDS)
                .withMaxRetries(maxAttempts);
        RecordParser parser = inputParser(config);

        // Setup processing task
        String path = config.getString("path");
        int chunk = config.getInteger("chunk", 8192);

        // Execute logic
        Failsafe.with(retryPolicy)
                .with(VertxScheduler.create(vertx))
                .runAsync(execution -> {
                    // Check if exist & attempt to read
                    Future<Boolean> action = Future.future();
                    vertx.fileSystem().exists(path, action.completer());
                    action.compose(exist -> {
                        Future<AsyncFile> subAction = Future.future();
                        if (exist) {
                            vertx.fileSystem().open(path, new OpenOptions().setRead(true), subAction.completer());
                        } else {
                            subAction.fail(StringUtils.EMPTY);
                        }
                        return subAction;
                    }).setHandler(h -> {
                        if (h.succeeded()) {
                            file = h.result();
                            file.setReadBufferSize(chunk);

                            // Handle back-pressure
                            handlePressure(file);

                            // Begin to read
                            file.handler(parser).exceptionHandler(THROWABLE_HANDLER);
                        } else if (!execution.retryOn(h.cause())) {
                            log.error(h.cause());
                        }
                    });
                });
    }

    @Override public void stop() {
        if (file != null) file.close();
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("path") != null, "The path is required");
        checkState(config.getString("dispatch") != null, "The dispatch is required");
    }

}