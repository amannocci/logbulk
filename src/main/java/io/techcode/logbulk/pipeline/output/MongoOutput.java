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

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.Flusher;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Mongo output pipeline component.
 */
public class MongoOutput extends BaseComponentVerticle {

    // Mongo client
    private MongoClient client;

    // Documents pending
    private JsonArray pending = new JsonArray();

    // Some settings
    private int bulk;
    private Flusher flusher;

    // Collection
    private String collection;

    // Settings
    private List<String> dates;

    @Override public void start() {
        super.start();

        // Setup processing task
        this.bulk = config.getInteger("bulk", 1000);
        this.collection = config.getString("collection");
        dates = Streams.to(config.getJsonArray("date", new JsonArray()).stream(), String.class).collect(Collectors.toList());

        // Setup mongo client
        JsonObject mongoConf = new JsonObject();
        mongoConf.put("db_name", config.getString("database"));
        mongoConf.put("connection_string", config.getString("uri"));
        client = MongoClient.createShared(vertx, mongoConf);

        // Setup flusher
        flusher = new Flusher(vertx, config.getInteger("flush", 10));
        flusher.handler(h -> send());
        flusher.start();

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Prepare body
        JsonObject body = packet.getBody();
        if (dates.size() > 0) {
            dates.stream()
                    .filter(body::containsKey)
                    .forEach(d -> body.put(d, new JsonObject().put("$date", body.getString(d))));
        }

        // Enqueue for bulk request
        pending.add(body);

        // If send needed
        if (pending.size() >= bulk) {
            send();
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    @Override public void stop() {
        if (client != null) client.close();
    }

    /**
     * Send a request.
     */
    private void send() {
        // Pause component
        pause();

        // Update flusher flag
        flusher.flushed();

        // If no work needed
        if (pending.isEmpty()) {
            // Resume component
            resume();
        } else {
            // Prepare request
            JsonObject command = new JsonObject();
            command.put("insert", collection);
            command.put("documents", pending);

            // Send request
            client.runCommand("insert", command, event -> {
                if (event.failed()) {
                    log.error("Failed to insert documents", event.cause());
                }

                // Resume component
                resume();
            });

            // Reset pending
            pending = new JsonArray();
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("uri") != null, "The uri is required");
        checkState(config.getString("database") != null, "The database is required");
        checkState(config.getString("collection") != null, "The collection is required");
    }

}
