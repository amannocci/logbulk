package io.techcode.logbulk.util.logging;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;
import io.vertx.core.json.JsonArray;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provides utilities for manipulating and examining <code>Throwable</code> objects.
 */
public final class ExceptionUtils {

    /**
     * Gets the stack trace from a Throwable as a JsonArray of String.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         The Throwable to be examined.
     * @return The stack trace.
     */
    public static JsonArray getStackTrace(JsonArray stacktrace, Throwable th) {
        checkNotNull(th, "The throwable can't be null");
        if (stacktrace == null) stacktrace = new JsonArray();

        // Add exceptions
        extractStacktrace(stacktrace, th);

        // Add suppressed exceptions, if any
        for (Throwable se : th.getSuppressed()) {
            extractStacktrace(stacktrace, se);
        }

        // Add cause, if any
        Throwable cause = th.getCause();
        if (cause != null) {
            extractStacktrace(stacktrace, cause);
        }
        return stacktrace;
    }

    /**
     * Gets the stack trace from a Throwable as a JsonArray of String.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         The Throwable to be examined.
     * @return The stack trace.
     */
    public static JsonArray getStackTrace(JsonArray stacktrace, IThrowableProxy th) {
        checkNotNull(th, "The throwable can't be null");
        if (stacktrace == null) stacktrace = new JsonArray();

        // Add exceptions
        extractStacktrace(stacktrace, th);

        // Add suppressed exceptions, if any
        for (IThrowableProxy se : th.getSuppressed()) {
            extractStacktrace(stacktrace, se);
        }

        // Add cause, if any
        IThrowableProxy cause = th.getCause();
        if (cause != null) {
            extractStacktrace(stacktrace, cause);
        }
        return stacktrace;
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
        stacktrace.add(th.getMessage());
        StackTraceElement[] trace = th.getStackTrace();
        for (StackTraceElement el : trace) {
            stacktrace.add("at " + el.toString());
        }
    }

    /**
     * Extract stacktrace informations from Throwable.
     *
     * @param stacktrace stacktrace to decorate.
     * @param th         throwable to process.
     */
    private static void extractStacktrace(JsonArray stacktrace, IThrowableProxy th) {
        stacktrace.add(th.getMessage());
        StackTraceElementProxy[] trace = th.getStackTraceElementProxyArray();
        for (StackTraceElementProxy el : trace) {
            stacktrace.add(el.toString());
        }
    }

}