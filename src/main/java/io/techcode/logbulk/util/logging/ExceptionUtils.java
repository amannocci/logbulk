/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017-2017
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

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import com.google.common.base.Strings;
import io.vertx.core.json.JsonArray;
import lombok.NonNull;

/**
 * Provides utilities for manipulating and examining <code>Throwable</code> objects.
 */
public final class ExceptionUtils {

    // Block constructor
    private ExceptionUtils() {
    }

    /**
     * Gets the stack trace from a Throwable as a JsonArray of String.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         The Throwable to be examined.
     * @return The stack trace.
     */
    public static JsonArray getStackTrace(JsonArray stacktrace, @NonNull Throwable th) {
        JsonArray stack = stacktrace != null ? stacktrace : new JsonArray();

        // Add exceptions
        extractStacktrace(stack, th);

        // Add suppressed exceptions, if any
        for (Throwable se : th.getSuppressed()) {
            extractStacktrace(stack, se);
        }

        // Add cause, if any
        Throwable cause = th.getCause();
        if (cause != null) {
            extractStacktrace(stack, cause);
        }
        return stack;
    }

    /**
     * Gets the stack trace from a Throwable as a JsonArray of String.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         The Throwable to be examined.
     * @return The stack trace.
     */
    public static JsonArray getStackTrace(JsonArray stacktrace, @NonNull IThrowableProxy th) {
        JsonArray stack = stacktrace != null ? stacktrace : new JsonArray();

        // Add exceptions
        extractStacktrace(stack, th);

        // Add suppressed exceptions, if any
        for (IThrowableProxy se : th.getSuppressed()) {
            extractStacktrace(stack, se);
        }

        // Add cause, if any
        IThrowableProxy cause = th.getCause();
        if (cause != null) {
            extractStacktrace(stack, cause);
        }
        return stack;
    }

    /**
     * Gets the stack trace from a Throwable as a JsonArray of String.
     *
     * @param th The Throwable to be examined.
     * @return The stack trace.
     */
    public static JsonArray getStackTrace(Throwable th) {
        return getStackTrace(null, th);
    }

    /**
     * Extract stacktrace informations from Throwable.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         throwable to process.
     */
    private static void extractStacktrace(JsonArray stacktrace, Throwable th) {
        // Add message if present
        String message = th.getMessage();
        if (!Strings.isNullOrEmpty(message)) {
            stacktrace.add(message);
        }

        // Handle stacktrace entries
        StackTraceElement[] trace = th.getStackTrace();
        if (trace != null) {
            for (StackTraceElement el : trace) {
                stacktrace.add("at " + el.toString());
            }
        }
    }

    /**
     * Extract stacktrace informations from Throwable.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         throwable to process.
     */
    private static void extractStacktrace(JsonArray stacktrace, IThrowableProxy th) {
        // Add message if present
        String message = th.getMessage();
        if (!Strings.isNullOrEmpty(message)) {
            stacktrace.add(message);
        }

        // Handle stacktrace entries
        StackTraceElementProxy[] trace = th.getStackTraceElementProxyArray();
        if (trace != null) {
            for (StackTraceElementProxy el : trace) {
                stacktrace.add(el.toString());
            }
        }
    }

}