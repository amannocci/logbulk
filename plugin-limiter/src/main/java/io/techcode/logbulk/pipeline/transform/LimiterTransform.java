/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2016-2017
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

import com.google.common.collect.Queues;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.vertx.core.json.JsonObject;

import java.util.Queue;

import static com.google.common.base.Preconditions.checkState;

/**
 * Limiter transformer pipeline component.
 */
public class LimiterTransform extends BaseComponentVerticle {

    // Request per second
    private long request = 0;

    // Limit
    private int limit;

    // Pending delivery
    private final Queue<Packet> pending = Queues.newArrayDeque();

    @Override public void start() {
        super.start();

        // Setup
        limit = config.getInteger("limit");

        // Flush everysecond
        vertx.setPeriodic(1000, h -> {
            if (request > limit) {
                if (pending.size() > limit) {
                    pending.stream().limit(limit).forEach(this::forward);
                    for (int i = 0; i < limit; i++) pending.poll();
                } else {
                    pending.forEach(this::forward);
                    pending.clear();
                    release();
                    request = 0;
                }
            } else {
                request = 0;
            }
        });

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Limit reached
        if (request > limit) {
            pending.add(packet);
        } else {
            request += 1;

            // Send to the next endpoint
            forwardAndRelease(packet);
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getInteger("limit") != null &&
                config.getInteger("limit") > 0, "The limit is required");
    }

}