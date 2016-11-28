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
package io.techcode.logbulk.pipeline.output;

import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.util.Flusher;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.Deque;
import java.util.Iterator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Elasticsearch output pipeline component.
 */
public class ElasticOutput extends BaseComponentVerticle {

    // Cyclable hosts
    private Iterator<String> hosts;

    // Http client to perform request
    private HttpClient http;

    // Some settings
    private int bulk;
    private Flusher flusher;

    // Stuff to build meta and request
    private Deque<JsonObject> pending = Queues.newArrayDeque();
    private String index;
    private String type;

    @Override public void start() {
        super.start();

        // Setup processing task
        hosts = Iterators.cycle(Streams.to(config.getJsonArray("hosts").stream(), String.class).collect(Collectors.toList()));
        this.bulk = config.getInteger("bulk", 1000);
        this.index = config.getString("index");
        this.type = config.getString("type");

        // Setup http client
        HttpClientOptions options = new HttpClientOptions();
        options.setTryUseCompression(true);
        options.setKeepAlive(true);
        options.setPipelining(true);
        this.http = vertx.createHttpClient(options);

        // Setup flusher
        flusher = new Flusher(vertx, config.getInteger("flush", 10));
        flusher.handler(h -> send());
        flusher.start();

        // Ready
        resume();
    }

    @Override public void stop() {
        if (http != null) http.close();
    }

    @Override public void handle(JsonObject msg) {
        // Add to pending message
        pending.add(msg);

        // Send if needed
        if (pending.size() >= bulk) {
            send();
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("index") != null, "The index is required");
        checkState(config.getString("type") != null, "The type is required");
        checkState(config.getJsonArray("hosts") != null
                && Streams.to(config.getJsonArray("hosts").stream(), String.class).count() > 0, "The hosts is required");
    }

    /**
     * Send a request.
     */
    private void send() {
        // Alreay paused
        if (isPause()) return;

        // Update flusher flag
        flusher.flushed();

        // If no work needed
        if (pending.size() > 0) {
            Deque<JsonObject> process = pending;
            pending = Queues.newArrayDeque();

            // Prepare header
            StringBuilder builder = new StringBuilder();
            for (JsonObject msg : process) {
                // Prepare header
                String idx = index;
                JsonObject body = body(msg);
                if (body.containsKey("_index")) {
                    idx += body.getString("_index");
                    body.remove("_index");
                }
                builder.append(new JsonObject().put("index", new JsonObject()
                        .put("_type", type)
                        .put("_index", idx)).encode()).append("\n");

                // Prepare body
                builder.append(body.encode()).append("\n");
            }

            // Prepare http request
            String payload = builder.toString();
            HttpClientRequest req = http.postAbs(hosts.next() + "/_bulk", res -> {
                // Handle request status
                if (res.statusCode() == 200) {
                    log.info("Bulk request: " + process.size() + " documents");
                    while (process.size() > 0) forward(process.removeLast());
                } else if (res.statusCode() == 429) {
                    log.error("Too many requests: statusCode=429");
                    while (process.size() > 0) refuse(process.removeLast());
                } else if (res.statusCode() == 503) {
                    log.error("Service unavailable: statusCode=503");
                    while (process.size() > 0) refuse(process.removeLast());
                } else {
                    log.error("Failed to index document: statusCode=" + res.statusCode());
                    while (process.size() > 0) refuse(process.removeLast());
                }

                // Resume component
                release();
            });
            req.setChunked(false);
            req.exceptionHandler(err -> {
                THROWABLE_HANDLER.handle(err);
                while (process.size() > 0) refuse(process.removeLast());
                release();
            });
            req.end(payload);
        }
    }

}