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
package io.techcode.logbulk.pipeline.input;

import com.google.common.base.Strings;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * Mysql input pipeline component.
 */
public class MysqlInput extends ComponentVerticle {

    // Async sql client
    private AsyncSQLClient client;

    // Read stream
    private DBReadStream stream;

    @Override public void start() {
        super.start();

        // Setup processing task
        String statement = config.getString("statement");
        JsonObject parameters = config.getJsonObject("parameters");
        JsonArray order = config.getJsonArray("order");
        client = MySQLClient.createShared(vertx, config);
        stream = new DBReadStream(client, statement, parameters, order, config.getString("track"), config.getBoolean("nonEmpty", false));

        // Setup periodic task
        handlePressure(stream);
        stream.handler(this::createEvent);
        stream.exceptionHandler(THROWABLE_HANDLER);
        stream.read();
    }

    @Override public void stop() {
        if (client != null) client.close();
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        checkState(config.getString("statement") != null, "The statement is required");
    }

    private class DBReadStream implements ReadStream<String> {

        // Async sql client
        private AsyncSQLClient client;

        // Statement
        private String statement;

        // Parameters & Order
        private JsonObject parameters;
        private JsonArray queryParams = new JsonArray();
        private boolean nonEmpty;
        private String track;
        private int trackPos = 0;

        // Paused & Running state
        private boolean paused = false;
        private boolean running = false;
        private boolean ended = false;

        // Handlers
        private Handler<String> handler;
        private Handler<Throwable> exceptionHandler;
        private Handler<Void> endHandler;

        /**
         * Create a new db read stream.
         *
         * @param client     async sql stream.
         * @param statement  statement to use.
         * @param parameters parameters to use.
         * @param order      parameters order.
         * @param track      column to track.
         * @param nonEmpty   not empty.
         */
        public DBReadStream(AsyncSQLClient client, String statement, JsonObject parameters, JsonArray order, String track, boolean nonEmpty) {
            checkArgument(!Strings.isNullOrEmpty(statement), "The statement can't be null or empty");
            checkNotNull(order, "The order can't be null");
            this.client = checkNotNull(client, "The client can't be null");
            this.parameters = checkNotNull(parameters, "The parameters can't be null");
            this.statement = statement;
            this.track = track;
            this.nonEmpty = nonEmpty;

            // Add offset and compute queryParams
            if (!parameters.containsKey("offset")) parameters.put("offset", 0);
            this.track = (Strings.isNullOrEmpty(track)) ? null : track;
            List<String> orderList = Streams.to(order.stream(), String.class).collect(Collectors.toList());
            for (int i = 0; i < orderList.size(); i++) {
                if (track != null && track.equalsIgnoreCase(orderList.get(i))) trackPos = i;
                queryParams.add(parameters.getValue(orderList.get(i)));
            }
        }

        /**
         * Initialize read on stream.
         */
        public void read() {
            if (running) return;
            running = true;
            client.getConnection(res -> {
                if (res.succeeded()) {
                    SQLConnection connection = res.result();

                    // Prepare parameters
                    if (track != null) queryParams.getList().set(trackPos, parameters.getValue(track));

                    // Got a connection
                    connection.queryWithParams(statement, queryParams, (AsyncResultHandler<ResultSet>) event -> {
                        try {
                            // Check if success
                            if (event.succeeded()) {
                                // Retrieve results
                                List<JsonObject> rows = event.result().getRows();

                                // For each create a message
                                int offset = parameters.getInteger("offset");
                                log.info("Fetch new entries (" + rows.size() + '/' + offset + ')');
                                if (handler != null) rows.forEach(e -> {
                                    if (track != null) parameters.put(track, e.getValue(track));
                                    if (nonEmpty) {
                                        for (Iterator<Map.Entry<String, Object>> it = e.iterator(); it.hasNext(); ) {
                                            Map.Entry<String, Object> entry = it.next();
                                            if (entry.getValue() == null) {
                                                it.remove();
                                            } else if (entry.getValue() instanceof String && "".equals(entry.getValue())) {
                                                it.remove();
                                            }
                                        }
                                    }
                                    handler.handle(e.encode());
                                });

                                // Handle end of stream
                                if (rows.isEmpty()) {
                                    ended = true;
                                    if (endHandler != null) endHandler.handle(null);
                                }

                                // Ensure to limit new values
                                parameters.put("offset", offset + rows.size());
                            } else {
                                // Catch errors
                                if (exceptionHandler != null) exceptionHandler.handle(event.cause());
                            }
                        } finally {
                            // Release connection to the pool.
                            connection.close();

                            // Flag running and execute if possible
                            running = false;
                            if (!paused && !ended) read();
                        }
                    });
                } else {
                    log.error("Can't establish connection to database");
                    running = false;
                    vertx.setTimer(5000, h -> read());
                }
            });
        }

        @Override public ReadStream<String> pause() {
            this.paused = true;
            return this;
        }

        @Override public ReadStream<String> resume() {
            if (paused) {
                this.paused = false;
                read();
            }
            return this;
        }

        @Override public ReadStream<String> exceptionHandler(Handler<Throwable> handler) {
            this.exceptionHandler = handler;
            return this;
        }

        @Override public ReadStream<String> handler(Handler<String> handler) {
            this.handler = handler;
            return this;
        }

        @Override public ReadStream<String> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }

    }

}
