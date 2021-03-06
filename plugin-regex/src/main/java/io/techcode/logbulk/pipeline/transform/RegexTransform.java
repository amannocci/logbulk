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

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.techcode.logbulk.util.logging.MessageException;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

/**
 * Regex transformer pipeline component.
 */
public class RegexTransform extends BaseComponentVerticle {

    // Settings
    private JsonPath source;
    private Pattern pattern;
    private Map<Integer, JsonPath> columns;
    private boolean nullable;

    @Override public void start() {
        super.start();

        // Setup
        source = JsonPath.create(config.getString("field"));
        nullable = config.getBoolean("nullable", true);
        String rawPattern = config.getString("pattern");
        JsonObject rawColumns = config.getJsonObject("columns");
        columns = Maps.newTreeMap();
        for (String key : rawColumns.fieldNames()) {
            Integer conv = Ints.tryParse(key);
            if (conv != null) columns.put(conv, JsonPath.create(rawColumns.getString(key)));
        }

        // Setup parser
        pattern = Pattern.compile(rawPattern);

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        JsonObject body = packet.getBody();
        String field = source.get(body, String.class);
        if (field != null) {
            Matcher matcher = pattern.matcher(field);
            if (matcher.matches()) {
                if (matcher.groupCount() >= columns.size()) {
                    for (int key : columns.keySet()) {
                        String col = matcher.group(key + 1);
                        JsonPath path = columns.get(key);

                        if (col == null) {
                            if (nullable) {
                                path.put(body, null);
                            } else {
                                path.remove(body);
                            }
                        } else {
                            path.put(body, col);
                        }
                    }
                }
                forwardAndRelease(packet);
            } else {
                handleFallback(packet, new MessageException("The field '" + source + "' can't be match"));
            }
        } else {
            handleFallback(packet, new MessageException("The field '" + source + "' can't be found"));
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("field") != null, "The field is required");
        checkState(config.getString("pattern") != null, "The pattern is required");
        checkState(config.getJsonObject("columns") != null, "The columns is required");
    }

}