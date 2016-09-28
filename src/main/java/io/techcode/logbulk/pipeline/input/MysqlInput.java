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
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.asyncsql.AsyncSQLClient;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.google.common.base.Preconditions.*;

/**
 * Mysql input pipeline component.
 */
@Slf4j
public class MysqlInput extends ComponentVerticle {

    // Async sql client
    private AsyncSQLClient client;

    // Read stream
    private DBReadStream stream;

    @Override public void start() {
        super.start();

        // Setup processing task
        String statement = config.getString("statement");
        int limit = config.getInteger("limit");
        int offset = config.getInteger("offset", 0);
        JsonArray parameters = config.getJsonArray("parameters", new JsonArray());
        client = MySQLClient.createShared(vertx, config);
        stream = new DBReadStream(client, limit, offset, statement, parameters);

        // Setup periodic task
        handlePressure(stream);
        stream.handler(this::createEvent);
        stream.exceptionHandler(h -> {
            log.error("Error during mysql read:", h.getCause());
            vertx.close();
        });
        stream.read();
    }

    @Override public void stop() {
        if (client != null) client.close();
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        checkState(config.getInteger("limit") != null && config.getInteger("limit") > 0, "The limit is required");
        if (config.getInteger("offset") != null) {
            checkState(config.getInteger("offset") >= 0, "The offset must be superior or equal to zero");
        }
        checkState(config.getString("statement") != null &&
                config.getString("statement").toLowerCase().contains("limit"), "The statement is required and must have a limit option");
    }

    private class DBReadStream implements ReadStream<String> {

        // Async sql client
        private AsyncSQLClient client;

        // Statement
        private String statement;

        // Parameters
        private JsonArray parameters;

        // Paused & Running state
        private boolean paused = false;
        private boolean running = false;

        // Limit & Offset
        private int limit;
        private int offset;

        // Handlers
        private Handler<String> handler;
        private Handler<Throwable> exceptionHandler;
        private Handler<Void> endHandler;

        /**
         * Create a new db read stream.
         *
         * @param client     async sql stream.
         * @param limit      limit size.
         * @param statement  statement to use.
         * @param parameters parameters to use.
         */
        public DBReadStream(AsyncSQLClient client, int limit, int offset, String statement, JsonArray parameters) {
            checkArgument(!Strings.isNullOrEmpty(statement), "The statement can't be null or empty");
            this.client = checkNotNull(client, "The client can't be null");
            this.limit = limit;
            this.offset = offset;
            this.statement = statement;
            this.parameters = parameters.add(limit).add(offset);
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
                    parameters.remove(parameters.size() - 1);
                    parameters.add(offset);

                    // Got a connection
                    connection.queryWithParams(statement, parameters, (AsyncResultHandler<ResultSet>) event -> {
                        try {
                            // Check if success
                            if (event.succeeded()) {
                                // Retrieve results
                                List<JsonObject> rows = event.result().getRows();

                                // For each create a message
                                log.info("Fetch new entries (" + rows.size() + '/' + offset + ')');
                                if (handler != null) rows.forEach(e -> handler.handle(e.encode()));

                                // Handle end of stream
                                if (rows.isEmpty() && endHandler != null) endHandler.handle(null);

                                // Ensure to limit new values
                                offset += limit;
                            } else {
                                // Catch errors
                                if (exceptionHandler != null) exceptionHandler.handle(event.cause());
                            }
                        } finally {
                            // Release connection to the pool.
                            connection.close();

                            // Flag running and execute if possible
                            running = false;
                            if (!paused) read();
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
            this.paused = false;
            read();
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
