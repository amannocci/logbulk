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
package io.techcode.logbulk.pipeline.output;

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.json.JsonObject;

/**
 * Standard output pipeline component.
 */
public class StdOutput extends BaseComponentVerticle {

    // Some constants
    private static final String CONF_FIELD = "field";

    // Settings
    private JsonPath field;

    @Override public void start() {
        super.start();

        // Setup
        field = config.containsKey(CONF_FIELD) ? JsonPath.create(config.getString(CONF_FIELD)) : null;

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        if (field != null) {
            log.info(field.get(packet.getBody()));
        } else {
            log.info(packet.getBody().encode());
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    @Override protected void checkConfig(JsonObject config) {
        // Nothing to check
    }

}
