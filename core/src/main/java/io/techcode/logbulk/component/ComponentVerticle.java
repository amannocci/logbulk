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
package io.techcode.logbulk.component;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.netty.buffer.ByteBufInputStream;
import io.techcode.logbulk.io.AppConfig;
import io.techcode.logbulk.io.Configuration;
import io.techcode.logbulk.net.FastJsonArrayCodec;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.PressureHandler;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Component verticle helper.
 */
public class ComponentVerticle extends AbstractVerticle {

    // Some constants
    private static final String MESSAGE = "message";
    private static final String STACKTRACE = "stacktrace";
    private static final String TRACES = "_traces";
    private static final String DISPATCH = "dispatch";
    private static final String DELIMITER = "delimiter";
    private static final String JSON = "json";

    // Fast json array
    private static final DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions().setCodecName(FastJsonArrayCodec.CODEC_NAME);

    // Logging
    protected static final Logger log = LoggerFactory.getLogger(ComponentVerticle.class);

    // Delivery options
    protected static final Handler<Throwable> THROWABLE_HANDLER = log::error;

    // UUID of the component
    protected final String uuid = UUID.randomUUID().toString();

    // Endpoint of the component
    protected String parentEndpoint;
    protected String parentWorkerEndpoint;
    protected String endpoint;

    // Routing
    protected ListMultimap<String, String> routing;

    // Event Bus
    @Getter private EventBus eventBus;

    // Configuration
    protected Configuration config;
    protected String fallback;
    private boolean tracing = true;

    // Mailbox
    private boolean hasMailbox = true;
    private int toRelease = 0;
    private JsonArray releaseOne;

    @Override public void start() {
        this.config = config();
        eventBus = vertx.eventBus();
        fallback = config.getString(AppConfig.FALLBACK, StringUtils.EMPTY);
        endpoint(config);
        releaseOne = new JsonArray().add(endpoint);

        // Settings
        JsonObject settings = new Configuration(config.getJsonObject(AppConfig.SETTING, new JsonObject()));
        tracing = settings.getBoolean(AppConfig.TRACING, false);

        // Generate routing
        JsonObject routes = config.getJsonObject(AppConfig.ROUTE);
        ImmutableListMultimap.Builder<String, String> builder = ImmutableListMultimap.builder();
        for (String route : routes.fieldNames()) {
            builder.putAll(route, Streams.to(routes.getJsonArray(route).stream(), String.class)
                    .collect(Collectors.toList()));
        }
        routing = builder.build();
    }

    /**
     * Check configuration arguments.
     * This method must be override to check configuration validity.
     *
     * @param config configuration to check.
     */
    protected void checkConfig(JsonObject config) {
        log.warn("Configuration not checked: " + getClass().getSimpleName());
    }

    @Override public Configuration config() {
        if (config == null) {
            config = new Configuration(super.config());
            checkConfig(config);
        }
        return config;
    }

    /**
     * Handle fallback during processing.
     *
     * @param packet packet involved.
     */
    public void handleFallback(Packet packet) {
        handleFallback(packet, null);
    }

    /**
     * Handle fallback during processing.
     *
     * @param packet packet involved.
     * @param th     error throw.
     */
    public void handleFallback(@NonNull Packet packet, Throwable th) {
        JsonObject body = packet.getBody();
        if (th != null) {
            body.put(STACKTRACE, ExceptionUtils.getStackTrace(th));
        }
        if (Strings.isNullOrEmpty(fallback)) {
            // Log info if no fallback
            log.error(packet.getBody());
            release();
        } else {
            // Otherwise send to fallback
            forwardAndRelease(updateRoute(packet, fallback));
        }
    }

    /**
     * Returns the previous component processor for this body.
     *
     * @param headers headers of the body.
     * @return previous component processor for this body.
     */
    public Optional<String> previous(@NonNull Packet.Header headers) {
        // Gets some stuff
        int previous = headers.getPrevious();

        // Possible previous component
        if (previous >= 0) {
            // Base on previous or current one
            String route = headers.getOldRoute();
            if (route == null) {
                route = headers.getRoute();
            }

            List<String> endpoints = this.routing.get(route);
            return Optional.of(endpoints.get(previous));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the next component processor for this packet.
     *
     * @param headers headers of the packet.
     * @return next component processor for this packet.
     */
    public Optional<String> next(Packet.Header headers) {
        return next(headers, headers.getCurrent());
    }

    /**
     * Returns the next component processor for this body.
     *
     * @param headers headers of the body.
     * @return next component processor for this body.
     */
    private Optional<String> next(@NonNull Packet.Header headers, int current) {
        // Gets some stuff
        int next = current + 1;

        // Retrieve routing
        String route = headers.getRoute();
        List<String> endpoints = this.routing.get(route);

        // Possible next component
        if (endpoints != null && next < endpoints.size()) {
            return Optional.of(endpoints.get(next));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Forward the packet to the next stage.
     *
     * @param packet packet to forward.
     */
    public void send(@NonNull Packet packet) {
        // Gets some stuff
        Packet.Header headers = packet.getHeader();
        int current = headers.getCurrent();

        // Determine next stage
        Optional<String> nextOpt = next(headers, current);
        if (nextOpt.isPresent()) {
            if (current > -1) {
                headers.setPrevious(current);
            }
            if (current == 0) {
                headers.setOldRoute(null);
            }
            headers.setCurrent(current + 1);

            // Add trace
            if (tracing) {
                JsonArray traces = headers.getJsonArray(TRACES);
                if (traces == null) {
                    traces = new JsonArray();
                    headers.put(TRACES, traces);
                }
                traces.add(endpoint);
            }
            eventBus.publish(nextOpt.get(), packet);
        }
    }

    /**
     * Forward the body to the next stage and release worker if mailbox.
     *
     * @param packet packet to forward.
     */
    public void forward(@NonNull Packet packet) {
        // Don't forget to release
        if (hasMailbox) {
            toRelease += 1;
        }

        // Send to next endpoint
        send(packet);
    }

    /**
     * Forward the packet to the next stage and release worker if mailbox.
     *
     * @param packet packet to forward.
     */
    public void forwardAndRelease(Packet packet) {
        forward(packet);
        release();
    }

    /**
     * Refuse the packet to process and send to mailbox.
     *
     * @param packet packet to refuse.
     */
    public void refuse(Packet packet) {
        Packet.Header headers = packet.getHeader();
        headers.setCurrent(headers.getCurrent() - 1);
        forward(packet);
    }

    /**
     * Notify to mailbox that worker is available.
     */
    public void release() {
        if (hasMailbox && toRelease > 0) {
            JsonArray msg = releaseOne;

            // If we release more than one, generate a custom message otherwise use already defined
            if (toRelease > 1) {
                msg = releaseOne.copy();
                msg.add(toRelease);
            }
            eventBus.publish(parentWorkerEndpoint, msg, DELIVERY_OPTIONS);
            toRelease = 0;
        }
    }

    /**
     * Update route of packet.
     *
     * @param packet packet to update.
     * @param route  route to use.
     * @return same object for chaining.
     */
    public Packet updateRoute(Packet packet, String route) {
        Packet.Header headers = packet.getHeader();
        headers.setOldRoute(headers.getRoute());
        headers.setRoute(route);
        headers.setPrevious(headers.getCurrent());
        headers.setCurrent(-1);
        return packet;
    }

    /**
     * Handle back-pressure on component.
     *
     * @param stream stream in read.
     */
    public void handlePressure(ReadStream stream) {
        handlePressure(stream, null);
    }

    /**
     * Handle back-pressure on component.
     *
     * @param stream     stream in read.
     * @param endHandler end handler to call.
     */
    public void handlePressure(ReadStream stream, Handler<Void> endHandler) {
        MessageConsumer<String> consumer = eventBus.consumer(parentEndpoint + ".pressure");
        consumer.handler(new PressureHandler(stream, parentEndpoint, h -> {
            if (endHandler != null) {
                endHandler.handle(null);
            }
            consumer.unregister();
        }));
    }

    /**
     * Returns a new input parser.
     *
     * @param config configuration details.
     * @return new input parser.
     */
    public RecordParser inputParser(JsonObject config) {
        boolean json = config.getBoolean(JSON, false);
        Handler<Buffer> handler = json ? buf -> {
            JsonObject message = decode(buf);
            if (!message.isEmpty()) {
                createEvent(message);
            }
        } : buf -> {
            String message = buf.toString();
            if (!Strings.isNullOrEmpty(message)) {
                createEvent(message);
            }
        };
        return RecordParser.newDelimited(config.getString(DELIMITER, "\n"), handler);
    }

    /**
     * Create a new packet.
     *
     * @param body message data.
     */
    protected final Packet generateEvent(String body) {
        return generateEvent(new JsonObject().put(MESSAGE, body));
    }

    /**
     * Create a new packet.
     *
     * @param body message data.
     */
    protected final Packet generateEvent(JsonObject body) {
        // Create a new body
        Packet.Header.HeaderBuilder headers = Packet.Header.builder();

        // Options
        if (!hasMailbox) {
            String dispatch = config.getString(DISPATCH);
            headers.route(dispatch);

            // Add source
            List<String> route = routing.get(dispatch);
            if (route.size() > 1) {
                headers.source(route.get(1));
            } else {
                headers.source(route.get(0));
            }
        }

        // Generate
        return Packet.builder()
                .header(headers.build())
                .body(body)
                .build();
    }

    /**
     * Create a new body and forwardAndRelease to next endpoint.
     *
     * @param body message data.
     */
    protected final void createEvent(JsonObject body) {
        // Send to the next endpoint
        forwardAndRelease(generateEvent(body));
    }

    /**
     * Create a new body and forwardAndRelease to next endpoint.
     *
     * @param body message data.
     */
    protected final void createEvent(String body) {
        createEvent(new JsonObject().put(MESSAGE, body));
    }

    /**
     * Notify pressure to another component.
     *
     * @param previousPressure already notified component.
     * @param headers          headers body involved.
     */
    public void notifyPressure(List<String> previousPressure, Packet.Header headers) {
        // Always return a previous in mailbox context
        Optional<String> previousOpt = previous(headers);
        previousOpt.ifPresent(e -> {
            // Handle pressure
            String previous = previousOpt.get();
            if (!previousPressure.contains(previous)) {
                if (previousPressure.isEmpty()) {
                    tooglePressure(previous);
                    previousPressure.add(previous);
                } else {
                    String old = previousPressure.remove(0);
                    String source = headers.getSource();
                    previousPressure.add(source);

                    if (!source.equals(old)) {
                        tooglePressure(source);
                        tooglePressure(old);
                    }
                }
            }
        });
    }

    /**
     * Toogle pressure state of the mailbox.
     *
     * @param endpoint endpoint to notify.
     */
    public void tooglePressure(String endpoint) {
        eventBus.publish(endpoint + ".pressure", parentEndpoint);
    }

    /**
     * Returns the unique endpoint of the component.
     *
     * @param config configuration.
     */
    public void endpoint(@NonNull JsonObject config) {
        parentEndpoint = config.getString(AppConfig.ENDPOINT);
        parentWorkerEndpoint = parentEndpoint + ".worker";

        if (config.getBoolean(AppConfig.HAS_MAILBOX, true)) {
            endpoint = parentWorkerEndpoint + '.' + uuid;
            eventBus.publish(parentWorkerEndpoint, new JsonArray().add(endpoint).add(0), DELIVERY_OPTIONS);
        } else {
            endpoint = parentEndpoint;
            hasMailbox = false;
        }
        log.info("Endpoint: " + endpoint);
    }

    /**
     * Decode a buffer into a json object.
     * Needed until vert.x 3.4.2 #1975
     *
     * @param buf buffer involved.
     * @return json object.
     */
    private JsonObject decode(Buffer buf) {
        ByteBufInputStream stream = new ByteBufInputStream(buf.getByteBuf());
        try {
            return new JsonObject((Map<String, Object>) Json.mapper.readValue(stream, Map.class));
        } catch (Exception e) {
            throw new DecodeException("Failed to decode:" + e.getMessage(), e);
        }
    }

}