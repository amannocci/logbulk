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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.techcode.logbulk.util.logging.MessageException;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Enclose transformer pipeline component.
 */
public class EncloseTransform extends BaseComponentVerticle {

    // Settings
    private JsonPath field;
    private char open;
    private char close;
    private String until;
    private String rest;
    private boolean preserve;
    private boolean trim;
    private int size = 0;
    private Map<Integer, String> columns;

    @Override public void start() {
        super.start();

        // Setup
        field = JsonPath.create(config.getString("field"));
        until = config.getString("until");
        if (Strings.isNullOrEmpty(until)) {
            open = config.getString("open").charAt(0);
            close = config.getString("close").charAt(0);
            rest = config.getString("rest");
            preserve = config.getBoolean("preserve", true);
        }
        trim = config.getBoolean("trim", false);
        JsonObject rawColumns = config.getJsonObject("columns");
        columns = Maps.newTreeMap();
        for (String key : rawColumns.fieldNames()) {
            Integer conv = Ints.tryParse(key);
            if (conv != null) {
                size = Math.max(size, conv);
                columns.put(conv, rawColumns.getString(key));
            }
        }

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        JsonObject body = packet.getBody();
        String source = field.get(body, String.class);
        if (source != null) {
            if (Strings.isNullOrEmpty(until)) {
                int start = 0;
                int end;
                int group = 0;

                for (int i = 0; i < source.length() && group <= size; i++) {
                    // Retrieve current character & set end
                    char ch = source.charAt(i);
                    end = i;

                    // If open
                    if (ch == open) {
                        // Handle previous if not enclose
                        if (preserve && end != 0 && source.charAt(i - 1) != close) {
                            group = setValue(body, group, source, start, end);
                        }
                        start = i + 1;
                    } else if (ch == close) {
                        // Process
                        group = setValue(body, group, source, start, end);
                        start = i + 1;
                    }
                }

                // Handle last case
                if (!Strings.isNullOrEmpty(rest)) {
                    body.put(rest, source.substring(start));
                }
            } else {
                int end = source.indexOf(until);
                if (end != -1) {
                    setValue(body, 0, source, 0, end);
                    setValue(body, 1, source, end, source.length());
                } else {
                    body.put(columns.get(0), source);
                }
            }

            // Forward to next component
            forwardAndRelease(packet);
        } else {
            handleFallback(packet, new MessageException("The field '" + this.field + "' can't be found"));
        }
    }

    /**
     * Set value to body.
     *
     * @param body  body involved.
     * @param group current group.
     * @param field field involved.
     * @param start start position.
     * @param end   end position.
     * @return new group value.
     */
    private int setValue(JsonObject body, int group, String field, int start, int end) {
        if (columns.containsKey(group)) {
            String value = field.substring(start, end);
            if (trim) {
                value = value.trim();
            }
            body.put(columns.get(group), value);
        }
        return group + 1;
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("field") != null, "The field is required");
        if (Strings.isNullOrEmpty(config.getString("until"))) {
            checkState(!Strings.isNullOrEmpty(config.getString("open")), "The open is required");
            checkState(!Strings.isNullOrEmpty(config.getString("close")), "The close is required");
        }
        checkState(config.getJsonObject("columns") != null, "The columns is required");
    }

}