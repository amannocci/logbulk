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
package io.techcode.logbulk.component;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * Mailbox implementation.
 */
public class Mailbox extends ComponentVerticle implements ConvertHandler {

    // Default threehold
    public final static int DEFAULT_THREEHOLD = 1000;

    // Treehold
    private int threehold;
    private int idle;

    // Workers
    private NavigableSet<String> workers = Sets.newTreeSet();
    private Map<String, Integer> workersJob = Maps.newHashMap();

    // Pending message to process
    private Queue<JsonObject> buffer = Queues.newArrayDeque();

    // Back pressure
    private Set<String> previousPressure = Sets.newHashSet();
    private Set<String> nextPressure = Sets.newHashSet();

    @Override public void start() {
        super.start();

        // Retrieve configuration settings
        threehold = config.getInteger("mailbox");
        idle = threehold / 2;
        int componentCount = config.getInteger("instance");
        threehold *= componentCount;

        // Setup
        getEventBus().<JsonObject>localConsumer(endpoint).handler(this).exceptionHandler(THROWABLE_HANDLER);
        getEventBus().<JsonArray>localConsumer(endpoint + ".worker").handler(event -> {
            // Decrease job
            String worker = event.body().getString(0);
            int job = workersJob.getOrDefault(worker, 0);
            job -= event.body().getInteger(1);
            workersJob.put(worker, job);

            // Check idle
            if (job < idle) workers.add(worker);

            // Check if there is work to be done
            processBuffer(Optional.of(worker));
        }).exceptionHandler(THROWABLE_HANDLER);
        getEventBus().<String>localConsumer(endpoint + ".pressure").handler(event -> {
            String component = event.body();
            if (nextPressure.contains(component)) {
                nextPressure.remove(component);
                if (nextPressure.isEmpty()) {
                    processBuffer(Optional.empty());
                }
            } else {
                nextPressure.add(component);
            }
        }).exceptionHandler(THROWABLE_HANDLER);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getInteger("mailbox") != null &&
                config.getInteger("mailbox") > 1, "The mailbox is required");
        checkState(config.getInteger("instance") != null &&
                config.getInteger("instance") > 0, "The instance is required");
    }

    @Override public void handle(JsonObject msg) {
        if (workers.isEmpty()) {
            handlePressure(msg);
        } else {
            Optional<String> nextOpt = next(headers(msg));
            if (nextOpt.isPresent() && nextPressure.contains(nextOpt.get())) {
                handlePressure(msg);
            } else {
                if (!process(msg, Optional.empty())) {
                    handlePressure(msg);
                }
            }
        }
    }

    /**
     * Add body in buffer and handle back pressure.
     *
     * @param msg message to add in buffer.
     */
    private void handlePressure(JsonObject msg) {
        buffer.add(msg);
        if (buffer.size() > threehold) {
            notifyPressure(previousPressure, headers(msg));
        }
    }

    /**
     * Send body to an available worker.
     *
     * @param msg       message to process.
     * @param workerOpt optional worker.
     * @return true if success, otherwise false.
     */
    private boolean process(JsonObject msg, Optional<String> workerOpt) {
        // Retrieve a worker
        String worker = workerOpt.orElseGet(() -> workers.isEmpty() ? null : workers.first());
        if (Strings.isNullOrEmpty(worker)) return false;

        // Increase job
        int job = workersJob.getOrDefault(worker, 0);
        workersJob.put(worker, ++job);

        // Evict if busy & send job
        if (job >= threehold) workers.remove(worker);
        getEventBus().send(worker, msg, DELIVERY_OPTIONS);
        return true;
    }

    /**
     * Attempt to process an body in the buffer.
     *
     * @param workerOpt optional worker.
     */
    private void processBuffer(Optional<String> workerOpt) {
        if (buffer.size() > 0) {
            JsonObject msg = buffer.poll();
            if (process(msg, workerOpt)) {
                // Handle pressure
                if (buffer.size() < idle && previousPressure.size() > 0) {
                    previousPressure.forEach(this::tooglePressure);
                    previousPressure.clear();
                }
            } else {
                handlePressure(msg);
            }
        }
    }

}