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

import com.google.common.collect.Lists;
import io.techcode.logbulk.component.ComponentVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Tcp input pipeline component.
 */
public class TcpInput extends ComponentVerticle {

    // TCP Server
    private NetServer server;

    // List of connection
    private List<NetSocket> connections = Lists.newLinkedList();

    @Override public void start() {
        super.start();

        // Setup processing task
        int port = config.getInteger("port");
        String host = config.getString("host", "0.0.0.0");
        server = vertx.createNetServer();

        // Handle all connections
        server.connectHandler(c -> {
            connections.add(c);
            c.handler(inputParser(config));
            c.exceptionHandler(THROWABLE_HANDLER);
            handlePressure(c, h -> connections.remove(c));
        });

        // Listen incoming connections
        server.listen(port, host, h -> {
            if (h.succeeded()) {
                log.info("Listening on port => " + host + ':' + port);
            } else {
                log.error("Can't listen on port => " + host + ':' + port, h.cause());
            }
        });
    }

    @Override public void stop() {
        server.close(h -> {
            connections.forEach(NetSocket::close);
            connections.clear();
        });
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        checkState(config.getInteger("port") != null, "The port is required");
    }

}
