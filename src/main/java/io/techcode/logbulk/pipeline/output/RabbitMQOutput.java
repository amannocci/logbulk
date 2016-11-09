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

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.util.Streams;
import io.vertx.core.Context;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * RabbitMQ output pipeline component.
 */
public class RabbitMQOutput extends BaseComponentVerticle {

    // RabbitMQ client
    private Channel rabbit;

    // Mode
    private Mode mode;

    // Exchange
    private String exchange;

    // Routing key
    private String routingKey;

    // Blocked
    private boolean blocked = true;

    // Vertx context
    private Context ctx;

    @Override public void start() {
        super.start();

        // Setup processing task
        mode = Mode.valueOf(config.getString("mode", "publish").toUpperCase());
        exchange = config.getString("exchange");
        routingKey = config.getString("routingKey");
        int interval = config.getInteger("interval", 1);
        int intervalMax = config.getInteger("intervalMax", 60);
        int maxAttempts = config.getInteger("maxAttempts", -1);
        ctx = vertx.getOrCreateContext();

        // Policies
        RetryPolicy retryPolicy = new RetryPolicy()
                .withBackoff(Duration.seconds(interval), Duration.seconds(intervalMax))
                .withMaxAttempts(maxAttempts);
        RecoveryPolicy recoveryPolicy = new RecoveryPolicy()
                .withBackoff(Duration.seconds(interval), Duration.seconds(intervalMax))
                .withMaxAttempts(maxAttempts);

        // Configure policies
        Config conf = new Config()
                .withConnectRetryPolicy(retryPolicy)
                .withChannelRetryPolicy(retryPolicy)
                .withChannelRecoveryPolicy(recoveryPolicy)
                .withConnectionRetryPolicy(retryPolicy)
                .withConnectionRecoveryPolicy(recoveryPolicy);

        // Prepare hosts params
        String[] hosts = Streams.to(config.getJsonArray("hosts", new JsonArray().add("localhost")).stream(), String.class)
                .collect(Collectors.toList())
                .toArray(new String[0]);

        // Configure connection options
        try {
            ConnectionOptions options = new ConnectionOptions();
            options.withUsername(config.getString("user", "user1"));
            options.withPassword(config.getString("password", "password1"));
            options.withHosts(hosts);
            options.withPort(config.getInteger("port", 5672));
            options.withVirtualHost(config.getString("virtualHost", "vhost1"));
            if (config.getBoolean("ssl", false)) options.withSsl();
            options.withConnectionTimeout(Duration.seconds(config.getInteger("connectionTimeout", 60)));

            // Create a new connection
            Connection connection = Connections.create(options, conf);
            connection.addBlockedListener(new BlockedListener() {
                @Override public void handleBlocked(String s) throws IOException {
                    ctx.runOnContext(h -> blocked = true);
                }

                @Override public void handleUnblocked() throws IOException {
                    ctx.runOnContext(h -> {
                        blocked = false;
                        release();
                    });
                }
            });

            // Create a new channel
            rabbit = connection.createChannel();
            blocked = false;
        } catch (Exception ex) {
            log.error("RabbitMQ can't be initialized: ", ex);
        }
    }

    @Override public void stop() {
        if (rabbit != null) {
            try {
                rabbit.close();
            } catch (IOException | TimeoutException e) {
                log.error("Error in RabbitMQ during closing: ", e);
            }
        }
    }

    @Override public void handle(JsonObject msg) {
        if (blocked) {
            refuse(msg);
            return;
        }

        // Switch over mode
        switch (mode) {
            case PUBLISH:
                try {
                    rabbit.basicPublish(exchange, routingKey, MessageProperties.PERSISTENT_BASIC, body(msg).encode().getBytes());
                    forwardAndRelease(msg);
                } catch (IOException ex) {
                    handleFailure(msg, ex);
                }
                break;
            case ACK: {
                JsonObject headers = headers(msg);
                if (headers.getLong("_rabbit_ack") != null) {
                    try {
                        rabbit.basicAck(headers.getLong("_rabbit_ack"), false);
                        forwardAndRelease(msg);
                    } catch (IOException ex) {
                        handleFailure(msg, ex);
                    }
                }
            }
            break;
            case NACK: {
                JsonObject headers = headers(msg);
                if (headers.getLong("_rabbit_ack") != null) {
                    try {
                        rabbit.basicNack(headers.getLong("_rabbit_ack"), false, true);
                        forwardAndRelease(msg);
                    } catch (IOException ex) {
                        handleFailure(msg, ex);
                    }
                }
            }
            break;
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        Mode.valueOf(config.getString("mode", "publish").toUpperCase());
        if (!config.getString("mode", "publish").equalsIgnoreCase("publish")) {
            checkState(config.getString("exchange") != null, "The exchange is required");
            checkState(config.getString("routingKey") != null, "The routingKey is required");
        }
    }

    /**
     * Mode.
     */
    private enum Mode {
        PUBLISH,
        ACK,
        NACK
    }

}
