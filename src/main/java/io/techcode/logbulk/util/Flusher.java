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
package io.techcode.logbulk.util;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import lombok.NonNull;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represents a flusher implementation.
 */
public class Flusher {

    // Vertx instance
    private final Vertx vertx;

    // Periodic flush
    private final long time;

    // Task Id
    private long taskId = -1;

    // Last flush
    private long lastFlush = 0;

    // Handler to call when needed
    private Handler<Void> handler;

    /**
     * Create a new flusher.
     *
     * @param vertx vertx instance.
     * @param time  flush time.
     */
    public Flusher(@NonNull Vertx vertx, long time) {
        checkArgument(time > 0, "The time can't be inferior to zero");
        this.vertx = vertx;
        this.time = time;
    }

    /**
     * Set an handler to call on flush.
     *
     * @param handler handler to call on flush.
     */
    public void handler(Handler<Void> handler) {
        this.handler = handler;
    }

    /**
     * Start the flusher.
     */
    public void start() {
        stop();
        taskId = vertx.setTimer(time, new Handler<Long>() {
            @Override public void handle(Long event) {
                if ((System.currentTimeMillis() - lastFlush) > TimeUnit.SECONDS.toMillis(time)) {
                    flush();
                }
                taskId = vertx.setTimer(time, this);
            }
        });
    }

    /**
     * Stop the flusher.
     */
    public void stop() {
        if (taskId != -1) {
            vertx.cancelTimer(taskId);
            taskId = -1;
        }
    }

    /**
     * Explictly flush.
     */
    public void flush() {
        if (handler != null) handler.handle(null);
        flushed();
    }

    /**
     * Notify flush has been done.
     */
    public void flushed() {
        lastFlush = System.currentTimeMillis();
    }

}