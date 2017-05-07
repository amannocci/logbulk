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
package io.techcode.logbulk.component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import io.techcode.logbulk.io.AppConfig;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.*;

import static com.google.common.base.Preconditions.checkState;

/**
 * Mailbox implementation.
 */
public class Mailbox extends ComponentVerticle implements ConvertHandler {

    // Default threshold
    public static final int DEFAULT_THRESHOLD = 1000;

    // Threshold
    private int threshold;
    private int idle;

    // Workers
    private final NavigableSet<Worker> workers = Sets.newTreeSet();
    private final Map<String, Worker> workersJob = Maps.newHashMap();

    // Pending packet to process
    private final Deque<Packet> buffer = Queues.newArrayDeque();
    private boolean fifo;

    // Back pressure
    private final List<String> previousPressure = Lists.newArrayListWithCapacity(1);
    private final Set<String> nextPressure = Sets.newHashSet();

    @Override public void start() {
        super.start();

        // Retrieve configuration settings
        threshold = config.getInteger(AppConfig.MAILBOX);
        fifo = config.getBoolean(AppConfig.FIFO, true);
        idle = Math.max(1, threshold / 2);
        int componentCount = config.getInteger(AppConfig.INSTANCE);
        threshold *= componentCount;

        // Setup
        getEventBus().<Packet>localConsumer(endpoint).handler(this);
        getEventBus().<JsonArray>localConsumer(endpoint + ".worker").handler(this::handleWorker);
        getEventBus().<String>localConsumer(endpoint + ".pressure").handler(this::handlePressure);
        getEventBus().<JsonObject>localConsumer(endpoint + ".status").handler(this::handleStatus);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getInteger(AppConfig.MAILBOX) != null &&
                config.getInteger(AppConfig.MAILBOX) > 0, "The mailbox is required");
        checkState(config.getInteger(AppConfig.INSTANCE) != null &&
                config.getInteger(AppConfig.INSTANCE) > 0, "The instance is required");
    }

    @Override public void handle(Packet packet) {
        handlePressure(packet);
        if (!workers.isEmpty()) {
            processBuffers();
        }
    }

    /**
     * Handle worker event.
     *
     * @param event event involved.
     */
    private void handleWorker(Message<JsonArray> event) {
        // Isolate body message
        JsonArray body = event.body();

        // Retrieve worker
        String workerName = body.getString(0);
        Worker worker = getOrCreate(workerName);

        // Remove from set before anything
        workers.remove(worker);

        // Decrease job
        worker.job -= (body.size() == 2) ? body.getInteger(1) : 1;
        workersJob.put(workerName, worker);

        // Check idle
        if (worker.job < idle) workers.add(worker);

        // Check if there is work to be done
        processBuffers();
    }

    /**
     * Handle pressure event.
     *
     * @param event event involved.
     */
    private void handlePressure(Message<String> event) {
        String component = event.body();
        if (nextPressure.contains(component)) {
            nextPressure.remove(component);
            if (nextPressure.isEmpty()) {
                processBuffers();
            }
        } else {
            nextPressure.add(component);
        }
    }

    /**
     * Handle status event.
     *
     * @param event event involved.
     */
    private void handleStatus(Message<JsonObject> event) {
        JsonObject workerStatus = new JsonObject();
        for (Worker worker : workers) {
            workerStatus.put(worker.name.substring(worker.name.length() - 36), worker.job);
        }

        JsonObject message = event.body();
        message.put(endpoint, new JsonObject()
                .put(AppConfig.MAILBOX, buffer.size())
                .put(AppConfig.IDLE, idle)
                .put(AppConfig.THRESHOLD, threshold)
                .put(AppConfig.WORKER, workerStatus));
        event.reply(message);
    }

    /**
     * Add packet in buffer and handle back pressure.
     *
     * @param packet packet to add in buffer.
     */
    private void handlePressure(Packet packet) {
        buffer.add(packet);
        if (buffer.size() > threshold) {
            notifyPressure(previousPressure, packet.getHeader());
        }
    }

    /**
     * Send packet to an available worker.
     *
     * @param packet packet to process.
     * @return true if success, otherwise false.
     */
    private boolean process(Packet packet) {
        // Retrieve a worker
        Worker worker = workers.isEmpty() ? null : workers.first();
        if (worker == null) {
            return false;
        }

        // Remove from set before anything
        workers.remove(worker);

        // Increase job
        worker.job++;

        // Evict if busy & send job
        if (worker.job < threshold) {
            workers.add(worker);
        }
        getEventBus().publish(worker.name, packet);
        return true;
    }

    /**
     * Attempt to process as much message possible in buffer.
     */
    private void processBuffers() {
        while (processBuffer()) ;
    }

    /**
     * Attempt to process an body in the buffer.
     */
    private boolean processBuffer() {
        if (!buffer.isEmpty()) {
            Packet packet = fifo ? buffer.pollFirst() : buffer.pollLast();
            Optional<String> nextOpt = next(packet.getHeader());

            if (nextOpt.isPresent()) {
                if (nextPressure.contains(nextOpt.get())) {
                    handlePressure(packet);
                } else {
                    return sendWorker(packet);
                }
            } else {
                if (nextPressure.isEmpty()) {
                    return sendWorker(packet);
                } else {
                    handlePressure(packet);
                }
            }
        }
        return false;
    }

    /**
     * Send the packet to the worker.
     *
     * @param packet packet to process.
     * @return true if success, otherwise false.
     */
    private boolean sendWorker(Packet packet) {
        if (process(packet)) {
            // Handle pressure
            if (buffer.size() < idle && !previousPressure.isEmpty()) {
                previousPressure.forEach(this::tooglePressure);
                previousPressure.clear();
            }
            return true;
        } else {
            handlePressure(packet);
            return false;
        }
    }

    /**
     * Get or create a worker based on his name.
     *
     * @param workerName worker name.
     * @return a worker.
     */
    private Worker getOrCreate(String workerName) {
        Worker worker = workersJob.get(workerName);
        if (worker != null) {
            return worker;
        } else {
            return new Worker(workerName, 0);
        }
    }

    /**
     * Worker component representation.
     */
    @AllArgsConstructor
    @Data
    private class Worker implements Comparable<Worker> {
        private final String name;
        private int job;

        @Override public int compareTo(Worker o) {
            return Integer.compare(job, o.job);
        }
    }

}