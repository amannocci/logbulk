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
package io.techcode.logbulk.pipeline.input;

import io.techcode.logbulk.component.ComponentVerticle;
import io.vertx.core.TimeoutStream;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;

/**
 * Heartbeat input pipeline component.
 */
@Slf4j
public class HeartbeatInput extends ComponentVerticle {

    @Override public void start() {
        super.start();

        // Setup processing task
        String message = config.getString("message");
        int interval = config.getInteger("interval", 1);

        // Setup periodic task
        TimeoutStream stream = vertx.periodicStream(interval * 1000)
                .handler(h -> createEvent(message));
        handlePressure(stream);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("message") != null, "The message is required");
    }

}