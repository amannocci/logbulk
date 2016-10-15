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

import com.google.common.collect.Sets;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.ConvertHandler;
import io.techcode.logbulk.util.Flusher;
import io.techcode.logbulk.util.Streams;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Mongo output pipeline component.
 */
@Slf4j
public class MongoOutput extends ComponentVerticle {

    // Mongo client
    private MongoClient client;

    // Documents pending
    private JsonArray pending = new JsonArray();

    // Some settings
    private int bulk;
    private Flusher flusher;

    // Back pressure
    private Set<String> previousPressure = Sets.newHashSet();
    private int threehold;
    private int idle;
    private int job = 0;
    private boolean paused;
    private String collection;

    @Override public void start() {
        super.start();

        // Setup processing task
        this.bulk = config.getInteger("bulk", 1000);
        this.threehold = config.getInteger("queue", 100);
        this.idle = threehold / 2;
        this.collection = config.getString("collection");
        List<String> dates = Streams.to(config.getJsonArray("date", new JsonArray()).stream(), String.class).collect(Collectors.toList());

        // Remap configuration
        JsonObject mongoConf = new JsonObject();
        mongoConf.put("db_name", config.getString("database"));
        mongoConf.put("connection_string", config.getString("uri"));
        client = MongoClient.createShared(vertx, mongoConf);
        flusher = new Flusher(vertx, config.getInteger("flush", 10));
        flusher.handler(h -> send());
        flusher.start();

        // Register endpoint
        getEventBus().<JsonObject>localConsumer(endpoint).handler((ConvertHandler) msg -> {
            JsonObject body = body(msg);
            if (dates.size() > 0) {
                dates.stream()
                        .filter(body::containsKey)
                        .forEach(d -> body.put(d, new JsonObject().put("$date", body.getString(d))));
            }
            pending.add(body);
            if (pending.size() >= bulk) {
                send();
            }
        });
    }

    @Override public void stop() {
        if (client != null) client.close();
    }

    /**
     * Send a request.
     */
    private void send() {
        flusher.flushed();
        if (pending.size() == 0 || paused) return;

        JsonObject command = new JsonObject();
        command.put("insert", collection);
        command.put("documents", pending);
        client.runCommand("insert", command, event -> {
            if (event.failed()) {
                log.error("Failed to insert documents");
            }
            release();
        });
        if (++job >= threehold && !paused) paused = true;

        // Reset pending
        pending = new JsonArray();
    }

    /**
     * Handle back pressure release.
     */
    private void release() {
        if (--job < idle && paused) {
            previousPressure.forEach(this::tooglePressure);
            previousPressure.clear();
            paused = false;
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("uri") != null, "The uri is required");
        checkState(config.getString("database") != null, "The database is required");
        checkState(config.getString("collection") != null, "The collection is required");
    }

}
