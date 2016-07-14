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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Mailbox implementation.
 */
@Slf4j
public class Mailbox extends ComponentVerticle implements Handler<JsonObject> {

    // Default threehold
    public final static int DEFAULT_THREEHOLD = 1000;

    // Treehold
    private int threehold;
    private int idle;

    // Pending message to process
    private Queue<JsonObject> buffer = Lists.newLinkedList();

    // Workers
    private NavigableSet<String> workers = Sets.newTreeSet();
    private Map<String, Integer> workersJob = Maps.newHashMap();

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
        getEventBus().<JsonObject>localConsumer(endpoint).handler(new ConvertHandler() {
            @Override public void handle(JsonObject message) {
                Mailbox.this.handle(message);
            }
        });
        getEventBus().<String>localConsumer(endpoint + ".worker").handler(event -> {
            // Decrease job
            String worker = event.body();
            int job = workersJob.getOrDefault(worker, 1);
            workersJob.put(worker, --job);

            // Check idle
            if (job < idle) workers.add(worker);

            // Check if there is work to be done
            processBuffer(Optional.of(event.body()));
        });
        getEventBus().<String>localConsumer(endpoint + ".pressure").handler(event -> {
            String component = event.body();
            if (nextPressure.contains(component)) {
                nextPressure.remove(component);
                processBuffer(Optional.empty());
            } else {
                nextPressure.add(component);
            }
        });
    }

    @Override public void handle(JsonObject msg) {
        if (workers.isEmpty()) {
            handlePressure(msg);
        } else {
            Optional<String> nextOpt = next(headers(msg));
            if (nextOpt.isPresent() && nextPressure.contains(nextOpt.get())) {
                handlePressure(msg);
            } else {
                process(msg, Optional.empty());
            }
        }
    }

    /**
     * Add event in buffer and handle back pressure.
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
     * Send event to an available worker.
     *
     * @param msg       message to process.
     * @param workerOpt optional worker.
     */
    private void process(JsonObject msg, Optional<String> workerOpt) {
        // Retrieve a worker
        String worker = workerOpt.orElseGet(() -> workers.isEmpty() ? null : workers.first());
        if (Strings.isNullOrEmpty(worker)) return;

        // Increase job
        int job = workersJob.getOrDefault(worker, 0);
        workersJob.put(worker, ++job);

        // Evict if busy & send job
        if (job >= threehold) workers.remove(worker);
        getEventBus().send(worker, msg, DELIVERY_OPTIONS);
    }

    /**
     * Attempt to process an event in the buffer.
     *
     * @param workerOpt optional worker.
     */
    private void processBuffer(Optional<String> workerOpt) {
        if (buffer.size() > 0) {
            process(buffer.poll(), workerOpt);

            // Handle pressure
            if (buffer.size() < idle && previousPressure.size() > 0) {
                previousPressure.forEach(this::tooglePressure);
                previousPressure.clear();
            }
        }
    }

}