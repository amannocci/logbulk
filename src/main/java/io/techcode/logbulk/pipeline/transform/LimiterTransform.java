/*
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

import com.google.common.collect.Sets;
import io.techcode.logbulk.component.TransformComponentVerticle;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * Limiter transformer pipeline component.
 */
@Slf4j
public class LimiterTransform extends TransformComponentVerticle {

    // Request per second
    private long request = 0;

    // Back pressure
    private Set<String> previousPressure = Sets.newHashSet();

    // Paused
    private boolean paused = false;

    // Limit
    private int limit;

    @Override public void start() {
        super.start();

        // Setup
        limit = config.getInteger("limit");

        // Flush everysecond
        vertx.setPeriodic(1000, h -> {
            request = 0;
            if (paused) {
                paused = false;
                previousPressure.forEach(this::tooglePressure);
                previousPressure.clear();
            }
        });
    }

    @Override public void handle(JsonObject msg) {
        // Process
        request += 1;

        // Limit reached ?
        if (request > limit && !paused) paused = true;

        // Paused
        if (paused) notifyPressure(previousPressure, headers(msg));

        // Send to the next endpoint
        forward(msg);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getInteger("limit") != null &&
                config.getInteger("limit") > 0, "The limit is required");
    }

}