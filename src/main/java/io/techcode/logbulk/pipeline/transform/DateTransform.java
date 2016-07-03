/**
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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.base.Strings;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.json.JsonObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkState;

/**
 * Date transformer pipeline component.
 */
public class DateTransform extends ComponentVerticle {

    // ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withLocale(Locale.ENGLISH);

    // Meta formatter
    private DateTimeFormatter metaFormatter;

    @Override public void start() {
        super.start();

        // Setup
        String target = config.getString("target", "@timestamp");
        String match = config.getString("match", "date");
        String meta = config.getString("meta");

        // Formatter
        DateTimeFormatter formatter = DateTimeFormat
                .forPattern(config.getString("format"))
                .withLocale(Locale.ENGLISH)
                .withDefaultYear(2016);

        // Meta formatter
        if (!Strings.isNullOrEmpty(meta)) {
            metaFormatter = DateTimeFormat
                    .forPattern(meta)
                    .withLocale(Locale.ENGLISH)
                    .withDefaultYear(2016);
        }

        // Register endpoint
        vertx.eventBus().<JsonObject>localConsumer(endpoint)
                .handler(new ConvertHandler() {
                    @Override public void handle(JsonObject msg) {
                        // Process
                        JsonObject evt = event(msg);
                        String field = evt.getString(match);
                        if (field != null) {
                            DateTime time = formatter.parseDateTime(field);
                            evt.put(target, ISO_FORMATTER.print(time));
                            if (metaFormatter != null) {
                                evt.put("_index", metaFormatter.print(time));
                            }
                        }

                        // Send to the next endpoint
                        forward(msg);
                    }
                });
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("match") != null, "The match is required");
        checkState(config.getString("format") != null, "The format is required");
        return config;
    }

}
