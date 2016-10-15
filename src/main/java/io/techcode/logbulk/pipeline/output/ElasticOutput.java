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
import com.google.common.collect.Sets;
import io.techcode.logbulk.component.TransformComponentVerticle;
import io.techcode.logbulk.util.Flusher;
import io.techcode.logbulk.util.Streams;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * File input pipeline component.
 */
@Slf4j
public class ElasticOutput extends TransformComponentVerticle {

    // Cyclable hosts
    private Iterator<String> hosts;

    // Bulk request builder
    private BulkRequestBuilder bulk;

    @Override public void start() {
        super.start();

        // Setup processing task
        hosts = Iterators.cycle(Streams.to(config.getJsonArray("hosts").stream(), String.class)
                .collect(Collectors.toList()));
        bulk = new BulkRequestBuilder(vertx, config);
    }

    @Override public void handle(JsonObject msg) {
        bulk.add(msg);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("index") != null, "The index is required");
        checkState(config.getString("type") != null, "The type is required");
        checkState(config.getJsonArray("hosts") != null
                && config.getJsonArray("hosts").size() > 0, "The hosts is required");
    }

    /**
     * Bulk request implementation.
     */
    private class BulkRequestBuilder {

        // Http client to perform request
        private HttpClient http;

        // Documents pending
        private int docs = 0;

        // Some settings
        private int bulk;
        private Flusher flusher;

        // Stuff to build meta and request
        private StringBuilder builder = new StringBuilder();
        private String index;
        private String type;

        // Back pressure
        private Set<String> previousPressure = Sets.newHashSet();
        private int threehold;
        private int idle;
        private int job = 0;
        private boolean paused;

        /**
         * Create a new bulk request builder.
         *
         * @param vertx  vertx instance.
         * @param config component configuration.
         */
        public BulkRequestBuilder(Vertx vertx, JsonObject config) {
            checkNotNull(vertx, "The vertx can't be null");
            checkNotNull(config, "The config can't be null");
            this.bulk = config.getInteger("bulk", 1000);
            this.index = config.getString("index");
            this.type = config.getString("type");
            this.threehold = config.getInteger("queue", 100);
            this.idle = threehold / 2;
            HttpClientOptions options = new HttpClientOptions();
            options.setTryUseCompression(true);
            options.setKeepAlive(true);
            options.setPipelining(true);
            options.setMaxPoolSize(config.getInteger("pool", HttpClientOptions.DEFAULT_MAX_POOL_SIZE));
            this.http = vertx.createHttpClient(options);
            flusher = new Flusher(vertx, config.getInteger("flush", 10));
            flusher.handler(h -> send());
            flusher.start();
        }

        /**
         * Add a document to the next flush
         *
         * @param msg message to process.
         */
        public void add(JsonObject msg) {
            String idx = index;
            JsonObject body = body(msg);
            if (body.containsKey("_index")) {
                idx += body.getString("_index");
                body.remove("_index");
            }
            if (paused) notifyPressure(previousPressure, headers(msg));
            builder.append(new JsonObject().put("index", new JsonObject()
                    .put("_type", type)
                    .put("_index", idx)).encode()).append("\n");
            builder.append(body.encode()).append("\n");
            if (++docs >= bulk) {
                send();
            }
        }

        /**
         * Send a request.
         */
        private void send() {
            flusher.flushed();
            if (docs == 0 || paused) return;

            int documents = docs;
            String payload = builder.toString();
            HttpClientRequest req = http.postAbs(hosts.next() + "/_bulk", res -> {
                if (res.statusCode() == 200) {
                    log.info("Bulk request: " + documents + " documents");
                } else if (res.statusCode() == 429) {
                    log.error("Too many requests: statusCode=429");
                } else if (res.statusCode() == 503) {
                    log.error("Service unavailable: statusCode=503");
                } else {
                    log.error("Failed to index document: statusCode=" + res.statusCode());
                }
                release();
            });
            req.setChunked(false);
            req.exceptionHandler(err -> {
                builder.append(payload);
                docs += documents;
                release();
            });
            req.end(payload);
            if (++job >= threehold && !paused) paused = true;

            // Reset size
            docs = 0;
            builder.setLength(0);
        }

        /**
         * Handle back pressure release.
         */
        private void release() {
            if (--job < idle && paused) {
                previousPressure.forEach(ElasticOutput.this::tooglePressure);
                previousPressure.clear();
                paused = false;
            }
        }

    }

}