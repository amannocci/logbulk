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
package io.techcode.logbulk.util.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.CoreConstants;
import ch.qos.logback.core.LayoutBase;
import com.google.common.base.Joiner;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Json layout implementation.
 */
public class JsonLayout extends LayoutBase<ILoggingEvent> {

    // Joiner
    private static final Joiner JOINER = Joiner.on('\n').skipNulls();

    boolean pretty = false;
    boolean stringify = false;

    public void setStringify(boolean stringify) {
        this.stringify = stringify;
    }

    public void setPretty(boolean pretty) {
        this.pretty = pretty;
    }

    @Override public String doLayout(ILoggingEvent event) {
        JsonObject log = new JsonObject();
        try {
            log.put("level", event.getLevel().toString());
            log.put("thread", event.getThreadName());
            log.put("timestamp", event.getTimeStamp());
            if (event.getMessage().startsWith("{") && event.getMessage().endsWith("}")) {
                log.mergeIn(new JsonObject(event.getMessage()));
            } else {
                log.put("message", event.getFormattedMessage());
            }

            // Handle internal vert.x stacktrace
            IThrowableProxy th = event.getThrowableProxy();
            if (th != null) {
                JsonArray stacktrace = ExceptionUtils.getStackTrace(log.getJsonArray("stacktrace"), th);
                if (stringify) {
                    log.put("stacktrace", JOINER.join(stacktrace));
                } else {
                    log.put("stacktrace", stacktrace);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace();
        }
        return ((pretty) ? log.encodePrettily() : log.encode()) + CoreConstants.LINE_SEPARATOR;
    }

}