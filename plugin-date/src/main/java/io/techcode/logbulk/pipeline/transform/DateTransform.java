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

import com.google.common.base.Strings;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.time.Year;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkState;

/**
 * Date transformer pipeline component.
 */
public class DateTransform extends BaseComponentVerticle {

    // ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withLocale(Locale.ENGLISH);

    // Settings
    private JsonPath target;
    private JsonPath field;
    private DateTimeFormatter formatter;
    private DateTimeFormatter metaFormatter;

    @Override public void start() {
        super.start();

        // Setup
        target = JsonPath.create(config.getString("target", "@timestamp"));
        field = config.containsKey("field") ? JsonPath.create(config.getString("field")) : null;
        String meta = config.getString("meta");

        // Formatter
        formatter = DateTimeFormat
                .forPattern(config.getString("format", "dd/MM/YYYY"))
                .withLocale(Locale.ENGLISH)
                .withDefaultYear(Year.now().getValue());

        // Meta formatter
        if (!Strings.isNullOrEmpty(meta)) {
            metaFormatter = DateTimeFormat
                    .forPattern(meta)
                    .withLocale(Locale.ENGLISH)
                    .withDefaultYear(Year.now().getValue());
        }
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        JsonObject body = packet.getBody();
        DateTime time = null;
        if (field == null) {
            time = new DateTime(System.currentTimeMillis());
        } else {
            String field = this.field.get(body, String.class);
            if (field != null) {
                time = formatter.parseDateTime(field);
            }
        }

        // We have a date
        if (time != null) {
            target.put(body, ISO_FORMATTER.print(time));
            if (metaFormatter != null) {
                body.put("_index", metaFormatter.print(time));
            }
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    @Override protected void checkConfig(JsonObject config) {
        if (config.getString("field") != null) {
            checkState(config.getString("format") != null, "The format is required");
        }
    }

}
