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

import com.rabbitmq.client.*;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import lombok.NonNull;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;

import java.io.IOException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * RabbitMQ output pipeline component.
 */
public class RabbitMQInput extends BaseComponentVerticle {

    // RabbitMQ client
    private Channel rabbit;

    // Vertx context
    private Context ctx;

    // Queue
    private String queue;

    // Auto ack
    private boolean autoAck;

    @Override public void start() {
        super.start();

        // Setup processing task
        queue = config.getString("queue");
        autoAck = config.getBoolean("autoAck", false);
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

            // Create a new channel
            rabbit = connection.createChannel();
            RabbitMQReadStream stream = new RabbitMQReadStream(rabbit);
            handlePressure(stream);
            stream.handler(this::forwardAndRelease);
            stream.exceptionHandler(THROWABLE_HANDLER);
            stream.resume();
        } catch (Exception ex) {
            log.error("RabbitMQ can't be initialized: ", ex);
        }

        // Register ack & nack consumer
        getEventBus().<Packet>localConsumer(endpoint + ".ack")
                .handler(packet -> {
                    Packet.Header headers = packet.body().getHeader();
                    if (headers.getLong("_rabbit_ack") != null) {
                        try {
                            rabbit.basicAck(headers.getLong("_rabbit_ack"), false);
                            packet.reply(null);
                        } catch (IOException ex) {
                            packet.fail(0, ex.getMessage());
                        }
                    }
                }).exceptionHandler(THROWABLE_HANDLER);
        getEventBus().<Packet>localConsumer(endpoint + ".nack")
                .handler(packet -> {
                    Packet.Header headers = packet.body().getHeader();
                    if (headers.getLong("_rabbit_ack") != null) {
                        try {
                            rabbit.basicNack(headers.getLong("_rabbit_ack"), false, true);
                            packet.reply(null);
                        } catch (IOException ex) {
                            packet.fail(0, ex.getMessage());
                        }
                    }
                }).exceptionHandler(THROWABLE_HANDLER);
    }

    @Override public void handle(Packet packet) {
        forwardAndRelease(packet);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        checkState(config.getString("queue") != null, "The queue is required");
    }

    private class RabbitMQReadStream implements ReadStream<Packet> {

        // RabbitMQ
        private final Channel rabbit;

        // Paused state
        private boolean paused = true;

        // Handlers
        private Handler<Packet> handler;
        private Handler<Throwable> exceptionHandler;
        private Handler<Void> endHandler;

        /**
         * Create a new db read stream.
         *
         * @param channel rabbitmq channel.
         */
        public RabbitMQReadStream(@NonNull Channel channel) {
            this.rabbit = channel;
        }

        @Override public ReadStream<Packet> pause() {
            if (!paused) {
                this.paused = true;
                try {
                    rabbit.basicCancel(uuid);
                } catch (IOException e) {
                    if (exceptionHandler != null) exceptionHandler.handle(e);
                }
            }
            return this;
        }

        @Override public ReadStream<Packet> resume() {
            if (paused) {
                this.paused = false;
                try {
                    rabbit.basicConsume(queue, autoAck, uuid, new DefaultConsumer(rabbit) {
                        @Override public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                            Packet packet = generateEvent(new String(body));
                            Packet.Header headers = packet.getHeader();
                            headers.put("_rabbit_ack", envelope.getDeliveryTag());
                            headers.put("_rabbit_source", endpoint);
                            ctx.runOnContext(h -> {
                                if (handler != null) handler.handle(packet);
                            });
                        }
                    });
                } catch (IOException e) {
                    if (exceptionHandler != null) exceptionHandler.handle(e);
                }
            }
            return this;
        }

        @Override public ReadStream<Packet> exceptionHandler(Handler<Throwable> handler) {
            this.exceptionHandler = handler;
            return this;
        }

        @Override public ReadStream<Packet> handler(Handler<Packet> handler) {
            this.handler = handler;
            return this;
        }

        @Override public ReadStream<Packet> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }
    }

}
