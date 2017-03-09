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
package io.techcode.logbulk.component;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.netty.handler.logging.LogLevel;
import io.techcode.logbulk.io.Configuration;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.PressureHandler;
import io.techcode.logbulk.util.logging.ExceptionUtils;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * Component verticle helper.
 */
public class ComponentVerticle extends AbstractVerticle {

    // Logging
    protected final Logger log = LoggerFactory.getLogger(getClass().getName());

    // Delivery options
    protected final Handler<Throwable> THROWABLE_HANDLER = log::error;

    // UUID of the component
    protected final String uuid = UUID.randomUUID().toString();

    // Endpoint of the component
    protected String parentEndpoint;
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

    @Override public void start() {
        this.config = config();
        eventBus = vertx.eventBus();
        fallback = config.getString("fallback", StringUtils.EMPTY);
        endpoint(config);

        // Settings
        JsonObject settings = new Configuration(config.getJsonObject("settings", new JsonObject()));
        tracing = settings.getBoolean("tracing", false);

        // Generate routing
        JsonObject routes = config.getJsonObject("route");
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
        Packet.Header headers = packet.getHeader();
        JsonObject body = packet.getBody();
        if (th != null) {
            JsonArray stacktrace = ExceptionUtils.getStackTrace(th);
            headers.put("_stacktrace", stacktrace);
            body.put("stacktrace", stacktrace);
            headers.put("_level", LogLevel.ERROR);
            body.put("level", LogLevel.ERROR);
        }
        if (Strings.isNullOrEmpty(fallback)) {
            // Log info if no fallback
            log.info(packet.getBody().encode());
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
            String route = headers.getOldRoute();
            if (route == null) route = headers.getRoute();
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
            if (current > -1) headers.setPrevious(current);
            if (current == 0) headers.setOldRoute(null);
            headers.setCurrent(current + 1);

            // Add trace
            if (tracing) {
                JsonArray traces = headers.getJsonArray("_traces");
                if (traces == null) {
                    traces = new JsonArray();
                    headers.put("_traces", traces);
                }
                traces.add(endpoint);
            }
            eventBus.send(nextOpt.get(), packet);
        }
    }

    /**
     * Forward the body to the next stage and release worker if mailbox.
     *
     * @param packet packet to forward.
     */
    public void forward(@NonNull Packet packet) {
        // Don't forget to release
        if (hasMailbox) toRelease += 1;

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
            JsonArray msg = new JsonArray().add(endpoint);
            if (toRelease > 1) msg.add(toRelease);
            eventBus.send(parentEndpoint + ".worker", msg);
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
        String endpoint = config.getString("endpoint");
        eventBus.<String>consumer(endpoint + ".pressure")
                .handler(new PressureHandler(stream, endpoint, endHandler));
    }

    /**
     * Returns a new input parser.
     *
     * @param config configuration details.
     * @return new input parser.
     */
    public RecordParser inputParser(JsonObject config) {
        return RecordParser.newDelimited(config.getString("delimiter", "\n"), buf -> createEvent(buf.toString()));
    }

    /**
     * Create a new packet.
     *
     * @param message message data.
     */
    protected final Packet generateEvent(String message) {
        // Create a new body
        Packet.Header.HeaderBuilder headers = Packet.Header.builder();
        JsonObject body = new JsonObject();
        if (!Strings.isNullOrEmpty(message)) {
            body.put("message", message);
        }

        // Options
        if (!hasMailbox) {
            String dispatch = config.getString("dispatch");
            headers.route(dispatch);

            // Add source
            List<String> route = routing.get(dispatch);
            if (route.size() > 1) {
                headers.source(route.get(1));
            } else{
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
     * @param message message data.
     */
    protected final void createEvent(String message) {
        // Send to the next endpoint
        forwardAndRelease(generateEvent(message));
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
        if (previousOpt.isPresent()) {

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
        } else {
            checkState(previousOpt.isPresent(), "There is always a previous component");
        }
    }

    /**
     * Toogle pressure state of the mailbox.
     *
     * @param endpoint endpoint to notify.
     */
    public void tooglePressure(String endpoint) {
        eventBus.send(endpoint + ".pressure", parentEndpoint);
    }

    /**
     * Returns the unique endpoint of the component.
     *
     * @param config configuration.
     */
    public void endpoint(@NonNull JsonObject config) {
        parentEndpoint = config.getString("endpoint");

        if (config.getBoolean("hasMailbox", true)) {
            endpoint = parentEndpoint + ".worker." + uuid;
            eventBus.send(parentEndpoint + ".worker", new JsonArray().add(endpoint).add(0));
        } else {
            endpoint = parentEndpoint;
            hasMailbox = false;
        }
        log.info("Endpoint: " + endpoint);
    }

}