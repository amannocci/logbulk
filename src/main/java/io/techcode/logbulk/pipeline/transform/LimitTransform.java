/**
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2016
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.techcode.logbulk.pipeline.transform;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.component.Mailbox;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;

/**
 * Limit transformer pipeline component.
 */
public class LimitTransform extends ComponentVerticle {

    // Pending events
    private Queue<JsonObject> pending = Lists.newLinkedList();

    // Source paused
    private List<String> sources = Lists.newLinkedList();

    @Override public void start() {
        // Configuration
        JsonObject config = config();

        // Rate limiter
        String endpoint = config.getString("endpoint");
        RateLimiter limiter = RateLimiter.create(config.getInteger("rate"));

        // Register endpoint
        vertx.eventBus().<JsonObject>consumer(endpoint)
                .handler(new Mailbox(this, endpoint, config.getInteger("mailbox", Mailbox.DEFAULT_THREEHOLD), evt -> {
                    // Process
                    if (limiter.tryAcquire()) {
                        // Send to the next endpoint
                        forward(evt);
                    } else {
                        // Limit the source and add to pending.
                        String source = source(evt);
                        pause(source, endpoint);
                        sources.add(source);
                        pending.add(evt);

                        // Retry
                        if (pending.size() == 1) {
                            vertx.setPeriodic(1000, h -> {
                                while (limiter.tryAcquire() && !pending.isEmpty()) {
                                    forward(pending.poll());
                                }
                                if (limiter.tryAcquire() && pending.isEmpty()) {
                                    for (String src : sources) {
                                        resume(src, endpoint);
                                    }
                                    sources.clear();
                                    vertx.cancelTimer(h);
                                }
                            });
                        }
                    }
                }));
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("endpoint") != null, "The endpoint is required");
        checkState(config.getInteger("rate") != null, "The rate is required");
        return config;
    }

}