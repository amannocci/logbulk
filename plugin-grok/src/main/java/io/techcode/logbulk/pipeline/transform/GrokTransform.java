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
package io.techcode.logbulk.pipeline.transform;

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.Match;
import io.thekraken.grok.api.exception.GrokException;
import io.vertx.core.json.JsonObject;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Grok transformer pipeline component.
 */
public class GrokTransform extends BaseComponentVerticle {

    // Settings
    private Grok grok;
    private JsonPath field;

    @Override public void start() {
        super.start();

        // Setup
        field = JsonPath.create(config.getString("field"));

        // Grok parser
        grok = new Grok();
        List<String> files = vertx.fileSystem().readDirBlocking(config.getString("path"));

        try {
            // Add all files in path
            for (String file : files) {
                grok.addPatternFromFile(file);
            }

            // Compile an expression
            grok.compile(config.getString("format"), true);

            // Ready
            resume();
        } catch (GrokException ex) {
            log.error("Can't instanciate grok:", ex);
        }
    }

    @Override public void handle(Packet packet) {
        // Process
        JsonObject body = packet.getBody();
        String source = field.get(body, String.class);
        if (source == null) {
            forwardAndRelease(packet);
        } else {
            Match matcher = grok.match(source);
            matcher.captures();
            if (matcher.isNull()) {
                handleFallback(packet);
            } else {
                // Compose
                body.mergeIn(new JsonObject(matcher.toMap()));

                // Send to the next endpoint
                forwardAndRelease(packet);
            }
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("path") != null, "The path is required");
        checkState(config.getString("field") != null, "The field is required");
        checkState(config.getString("format") != null, "The format is required");
    }

}