/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
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
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import lombok.Getter;

import static com.google.common.base.Preconditions.checkState;

/**
 * Tcp output pipeline component.
 */
public class TcpOutput extends BaseComponentVerticle {

    // Some constants
    private static final String CONF_PORT = "port";
    private static final String CONF_HOST = "host";
    private static final String CONF_FIELD = "field";
    private static final String CONF_DELIMITER = "delimiter";

    // TCP client
    private NetClient client;

    // Connection
    @Getter private NetSocket connection;

    // Settings
    @Getter private int port;
    private String host;
    private boolean isRunning = true;

    // Encoder
    private Handler<JsonObject> encoder;

    // Handle pressure
    private final Handler<Void> HANDLE_PRESSURE = h -> resume();

    @Override public void start() {
        super.start();

        // Setup processing task
        port = config.getInteger(CONF_PORT);
        host = config.getString(CONF_HOST, "0.0.0.0");
        encoder = encoder();
        client = vertx.createNetClient(new NetClientOptions()
                .setReconnectAttempts(Integer.MAX_VALUE)
                .setReconnectInterval(1000));

        // Fire hook
        onStart();

        // Handle connection
        connect();
    }

    /**
     * Hook for start.
     */
    protected void onStart() {
    }

    /**
     * Hook for stop.
     */
    protected void onStop() {
    }

    @Override public void handle(Packet packet) {
        encoder.handle(packet.getBody());

        // Overflow
        if (connection.writeQueueFull()) {
            // Pause component
            pause();
            connection.drainHandler(HANDLE_PRESSURE);
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    /**
     * Create a new default encoder.
     *
     * @return default encoder.
     */
    protected Handler<JsonObject> encoder() {
        String delimiter = config.getString(CONF_DELIMITER, "\n");
        JsonPath field = config.containsKey(CONF_FIELD) ? JsonPath.create(config.getString(CONF_FIELD)) : null;
        return (field != null) ? evt -> {
            connection.write(String.valueOf(field.get(evt))).write(delimiter);
        } : evt -> {
            connection.write(evt.encode()).write(delimiter);
        };
    }

    /**
     * Connect to the target.
     */
    private void connect() {
        client.connect(port, host, res -> {
            if (res.succeeded()) {
                log.info("Connected to '" + host + ':' + port);
                connection = res.result();
                connection.exceptionHandler(THROWABLE_HANDLER);
                connection.endHandler(h -> {
                    // Pause process
                    pause();
                    connection = null;
                    if (isRunning) connect();
                });

                // Resume process
                resume();
            } else {
                log.error("Failed to connect '" + host + ':' + port);
            }
        });
    }

    @Override public void stop() {
        isRunning = false;
        onStop();
        if (connection != null) connection.close();
        client.close();
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getInteger(CONF_PORT) != null, "The port is required");
    }

}
