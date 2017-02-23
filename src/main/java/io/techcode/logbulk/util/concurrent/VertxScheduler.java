/*
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
package io.techcode.logbulk.util.concurrent;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import net.jodah.failsafe.util.concurrent.DefaultScheduledFuture;
import net.jodah.failsafe.util.concurrent.Scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An adapter that workaround vertx scheduler.
 */
@Value
public class VertxScheduler implements Scheduler {

    // Vertx instance.
    private Vertx vertx;

    /**
     * Create a new vertx scheduler.
     *
     * @param vertx vertx instance.
     * @return a new vertx scheduler.
     */
    public static VertxScheduler create(@NonNull Vertx vertx) {
        return new VertxScheduler(vertx);
    }

    @Override public ScheduledFuture<?> schedule(Callable<?> callable, long delay, TimeUnit unit) {
        return new VertxScheduledFuture(callable, delay, unit);
    }

    /**
     * An implementation of ScheduledFuture for vertx.
     */
    private class VertxScheduledFuture extends DefaultScheduledFuture<Object> {

        // Context of run
        private final Context ctx;

        // Some properties
        private long timerId;
        private final Callable<?> callable;
        private final long delay;
        private final TimeUnit unit;

        /**
         * Create a new vertx scheduled future.
         *
         * @param callable logic to execute.
         * @param delay    delay before execution.
         * @param unit     unit of delay.
         */
        public VertxScheduledFuture(Callable<?> callable, long delay, TimeUnit unit) {
            ctx = vertx.getOrCreateContext();
            this.callable = callable;
            this.delay = delay;
            this.unit = unit;
            run();
        }

        @Override public long getDelay(TimeUnit unit) {
            return unit.toMillis(delay);
        }

        /**
         * Run logic.
         */
        private void call() {
            try {
                callable.call();
            } catch (Throwable th) {
                throw new RuntimeException(th);
            }
        }

        /**
         * Launch logic.
         */
        private void run() {
            if (getDelay(unit) == 0) {
                ctx.runOnContext(e -> call());
            } else {
                timerId = vertx.setTimer(getDelay(unit), tid -> call());
            }
        }

        @Override public boolean cancel(boolean mayInterruptIfRunning) {
            return delay != 0 && vertx.cancelTimer(timerId);
        }
    }

}
