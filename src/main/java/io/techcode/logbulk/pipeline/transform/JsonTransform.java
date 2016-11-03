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

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkState;

/**
 * Json transformer pipeline component.
 */
public class JsonTransform extends BaseComponentVerticle {

    // Settings
    private String source;

    @Override public void start() {
        super.start();

        // Setup
        source = config.getString("source");
    }

    @Override public void handle(JsonObject msg) {
        // Process
        JsonObject body = body(msg);
        body.mergeIn(new JsonObject(body.getString(source)));

        // Send to the next endpoint
        forwardAndRelease(msg);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("source") != null, "The source is required");
    }

}