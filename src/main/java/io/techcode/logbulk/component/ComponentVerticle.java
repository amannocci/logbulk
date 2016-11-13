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
import io.techcode.logbulk.io.Configuration;
import io.techcode.logbulk.util.PressureHandler;
import io.techcode.logbulk.util.Streams;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Component verticle helper.
 */
public class ComponentVerticle extends AbstractVerticle {

    // Logging
    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    // Delivery options
    protected static final DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions().setCodecName("fastjsonobject");

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

    // Mailbox
    private boolean hasMailbox = true;
    private int toRelease = 0;

    @Override public void start() {
        this.config = config();
        eventBus = vertx.eventBus();
        fallback = config.getString("fallback", StringUtils.EMPTY);
        endpoint(config);

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
     * Handle failure during processing.
     *
     * @param msg message involved.
     */
    public void handleFailure(JsonObject msg) {
        handleFailure(msg, null);
    }

    /**
     * Handle failure during processing.
     *
     * @param msg message involved.
     * @param th  error throw.
     */
    public void handleFailure(JsonObject msg, Throwable th) {
        if (th != null) {
            String[] stackFrames = ExceptionUtils.getStackFrames(th);
            JsonArray traces = new JsonArray();
            Arrays.asList(stackFrames).forEach(traces::add);
            body(msg).put("stacktrace", traces);
        }
        forwardAndRelease(updateRoute(msg, fallback));
    }

    /**
     * Extract headers part of the message.
     *
     * @param message message to process.
     * @return headers part.
     */
    public JsonObject headers(JsonObject message) {
        return message.getJsonObject("headers");
    }

    /**
     * Extract body part of the message.
     *
     * @param message message to process.
     * @return body part.
     */
    public JsonObject body(JsonObject message) {
        return message.getJsonObject("body");
    }

    /**
     * Returns the previous component processor for this body.
     *
     * @param headers headers of the body.
     * @return previous component processor for this body.
     */
    public Optional<String> previous(JsonObject headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int previous = headers.getInteger("_previous", -1);

        // Possible previous component
        if (previous >= 0) {
            String route = headers.getString("_route_old");
            if (route == null) route = headers.getString("_route");
            List<String> endpoints = this.routing.get(route);
            return Optional.of(endpoints.get(previous));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the next component processor for this body.
     *
     * @param headers headers of the body.
     * @return next component processor for this body.
     */
    public Optional<String> next(JsonObject headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int current = headers.getInteger("_current", 0);
        return next(headers, current);
    }

    /**
     * Returns the next component processor for this body.
     *
     * @param headers headers of the body.
     * @return next component processor for this body.
     */
    private Optional<String> next(JsonObject headers, int current) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int next = current + 1;

        // Retrieve routing
        String route = headers.getString("_route");
        List<String> endpoints = this.routing.get(route);

        // Possible next component
        if (endpoints != null && next < endpoints.size()) {
            return Optional.of(endpoints.get(next));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Forward the body to the next stage and release worker if mailbox.
     *
     * @param msg message to forward.
     */
    public void forward(JsonObject msg) {
        checkNotNull(msg, "The message to forward can't be null");

        // Don't forget to release
        if (hasMailbox) toRelease += 1;

        // Gets some stuff
        JsonObject headers = headers(msg);
        int current = headers.getInteger("_current", 0);

        // Determine next stage
        Optional<String> nextOpt = next(headers, current);
        if (nextOpt.isPresent()) {
            if (current > -1) headers.put("_previous", current);
            if (current == 0) headers.remove("_route_old");
            headers.put("_current", current + 1);
            eventBus.send(nextOpt.get(), msg, DELIVERY_OPTIONS);
        }
    }

    /**
     * Forward the body to the next stage and release worker if mailbox.
     *
     * @param msg message to forward.
     */
    public void forwardAndRelease(JsonObject msg) {
        forward(msg);
        release();
    }

    /**
     * Refuse the message to process and send to mailbox.
     *
     * @param msg message to refuse.
     */
    public void refuse(JsonObject msg) {
        JsonObject headers = headers(msg);
        headers.put("_current", headers.getInteger("_current") - 1);
        forward(msg);
    }

    /**
     * Notify to mailbox that worker is available.
     */
    public void release() {
        if (hasMailbox) {
            eventBus.send(parentEndpoint + ".worker", new JsonArray().add(endpoint).add(toRelease));
            toRelease = 0;
        }
    }

    /**
     * Update route of message.
     *
     * @param msg   message to update.
     * @param route route to use.
     * @return same object for chaining.
     */
    public JsonObject updateRoute(JsonObject msg, String route) {
        JsonObject headers = headers(msg);
        headers.put("_route_old", headers.getString("_route"));
        headers.put("_route", route);
        headers.put("_previous", headers.getInteger("_current"));
        headers.put("_current", -1);
        return msg;
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
     * Create a new message.
     */
    protected final JsonObject generateEvent() {
        return generateEvent(null);
    }

    /**
     * Create a new message.
     *
     * @param message message data.
     */
    protected final JsonObject generateEvent(String message) {
        // Create a new body
        JsonObject headers = new JsonObject();
        JsonObject body = new JsonObject();
        if (!Strings.isNullOrEmpty(message)) {
            body.put("message", message);
        }

        // Options
        headers.put("_route", config.getString("dispatch"));
        headers.put("_current", 0);

        // Send to the next endpoint
        return new JsonObject()
                .put("headers", headers)
                .put("body", body);
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
    public void notifyPressure(Set<String> previousPressure, JsonObject headers) {
        // Always return a previous in mailbox context
        Optional<String> previousOpt = previous(headers);
        if (previousOpt.isPresent()) {

            // Handle pressure
            String previous = previousOpt.get();
            if (!previousPressure.contains(previous)) {
                tooglePressure(previous);
                previousPressure.add(previous);
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
    public void endpoint(JsonObject config) {
        checkNotNull(config, "The configuration can't be null");
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