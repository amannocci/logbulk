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
package io.techcode.logbulk.util.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;
import io.vertx.core.json.JsonObject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for JsonLayout.
 */
public class JsonLayoutTest {

    private JsonObject excepted;
    private LoggingEvent event;

    @Before public void setUp() {
        String message = "This is a test";
        long timestamp = System.currentTimeMillis();
        event = new LoggingEvent();
        event.setLevel(Level.INFO);
        event.setMessage(message);
        event.setThreadName(Thread.currentThread().getName());
        event.setTimeStamp(timestamp);
        excepted = new JsonObject()
                .put("level", Level.INFO.toString())
                .put("thread", Thread.currentThread().getName())
                .put("timestamp", timestamp)
                .put("message", message);
    }

    @Test public void testDoLayout() throws Exception {
        JsonLayout layout = new JsonLayout();
        layout.setPretty(false);
        String result = layout.doLayout(event);
        assertEquals(excepted.encode() + System.lineSeparator(), result);
    }

    @Test public void testDoLayoutPretty() throws Exception {
        JsonLayout layout = new JsonLayout();
        layout.setPretty(true);
        String result = layout.doLayout(event);
        assertEquals(excepted.encodePrettily() + System.lineSeparator(), result);
    }

}
