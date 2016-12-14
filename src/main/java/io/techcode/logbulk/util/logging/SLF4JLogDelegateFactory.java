package io.techcode.logbulk.util.logging;

import io.vertx.core.spi.logging.LogDelegate;
import io.vertx.core.spi.logging.LogDelegateFactory;

public class SLF4JLogDelegateFactory implements LogDelegateFactory {
    public LogDelegate createDelegate(final String clazz) {
        return new SLF4JLogDelegate(clazz);
    }
}