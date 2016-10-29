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
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;

/**
 * RabbitMQ output pipeline component.
 */
@Slf4j
public class RabbitMQOutput extends BaseComponentVerticle {

    // RabbitMQ client
    private RabbitMQClient rabbit;

    // Mode ack
    private boolean modeAck;

    // Exchange
    private String exchange;

    // Queue
    private String queue;

    @Override public void start() {
        super.start();

        // Setup processing task
        modeAck = config.getBoolean("modeAck", false);
        exchange = config.getString("exchange");
        queue = config.getString("queue");
        rabbit = RabbitMQClient.create(vertx, config);

        // Attempt to connect
        rabbit.start(h -> {
            if (h.failed()) handleFailure(generateEvent(), h.cause());
        });
    }

    @Override public void handle(JsonObject msg) {
        if (modeAck) {
            JsonObject headers = headers(msg);
            if (headers.getLong("_rabbit_ack") != null) {
                rabbit.basicAck(headers.getLong("_rabbit_ack"), false, h -> {
                    if (h.failed()) {
                        handleFailure(msg, h.cause());
                    } else {
                        forwardAndRelease(msg);
                    }
                });
            }
        } else {
            rabbit.basicPublish(exchange, queue, new JsonObject()
                    .put("properties", new JsonObject().put("contentType", "application/json"))
                    .put("body", body(msg)), h -> {
                if (h.failed()) {
                    handleFailure(msg, h.cause());
                } else {
                    forwardAndRelease(msg);
                }
            });
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        if (!config.getBoolean("modeAck", false)) {
            checkState(config.getString("exchange") != null, "The exchange is required");
            checkState(config.getString("queue") != null, "The queue is required");
        }
    }

}