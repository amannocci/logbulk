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

import com.google.common.collect.ArrayListMultimap;
import io.techcode.logbulk.util.PressureHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Component verticle helper.
 */
@Slf4j
public class ComponentVerticle extends AbstractVerticle {

    // Delivery options
    protected static final DeliveryOptions DELIVERY_OPTIONS = new DeliveryOptions().setCodecName("fastjsonobject");

    // UUID of the component
    protected final String uuid = UUID.randomUUID().toString();

    // Endpoint of the component
    protected String parentEndpoint;
    protected String endpoint;

    // Routing
    protected ArrayListMultimap<String, String> routing = ArrayListMultimap.create();

    // Event Bus
    @Getter private EventBus eventBus;

    // Configuration
    protected JsonObject config;

    // Mailbox
    private boolean hasMailbox = true;

    @Override public void start() {
        this.config = config();
        eventBus = vertx.eventBus();
        endpoint(config);

        // Generate routing
        JsonObject routes = config.getJsonObject("route");
        for (String route : routes.fieldNames()) {
            routing.putAll(route, routes.getJsonArray(route).getList());
        }
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

    @Override public JsonObject config() {
        if (config == null) {
            config = super.config();
            checkConfig(config);
        }
        return config;
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
     * Extract event part of the message.
     *
     * @param message message to process.
     * @return event part.
     */
    public JsonObject event(JsonObject message) {
        return message.getJsonObject("event");
    }

    /**
     * Returns the previous component processor for this event.
     *
     * @param headers headers of the event.
     * @return previous component processor for this event.
     */
    public Optional<String> previous(JsonObject headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int current = headers.getInteger("_current", 0);
        int previous = current - 1;

        // Possible previous component
        if (previous >= 0) {
            String route = headers.getString("_route");
            List<String> endpoints = this.routing.get(route);
            return Optional.of(endpoints.get(previous));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns the next component processor for this event.
     *
     * @param headers headers of the event.
     * @return next component processor for this event.
     */
    public Optional<String> next(JsonObject headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int current = headers.getInteger("_current", 0);
        return next(headers, current);
    }

    /**
     * Returns the next component processor for this event.
     *
     * @param headers headers of the event.
     * @return next component processor for this event.
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
     * Forward the event to the next stage and release worker if mailbox.
     *
     * @param msg message to forward.
     */
    public void forward(JsonObject msg) {
        checkNotNull(msg, "The message to forward can't be null");

        // Gets some stuff
        JsonObject headers = headers(msg);
        int current = headers.getInteger("_current", 0);

        // Determine next stage
        Optional<String> nextOpt = next(headers, current);
        if (nextOpt.isPresent()) {
            headers.put("_current", current + 1);
            eventBus.send(nextOpt.get(), msg, DELIVERY_OPTIONS);
        }
        if (hasMailbox) eventBus.send(parentEndpoint + ".worker", endpoint);
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
        headers.put("_route", route);
        headers.put("_current", 0);
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
     * Create a new event and forward to next endpoint.
     *
     * @param message message data.
     */
    protected final void createEvent(String message) {
        // Create a new event
        JsonObject headers = new JsonObject();
        JsonObject evt = new JsonObject().put("message", message);

        // Options
        headers.put("_route", config.getString("dispatch"));
        headers.put("_current", 0);

        // Send to the next endpoint
        forward(new JsonObject()
                .put("headers", headers)
                .put("event", evt)
        );
    }

    /**
     * Notify pressure to another component.
     *
     * @param previousPressure already notified component.
     * @param headers          headers event involved.
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
            eventBus.send(parentEndpoint + ".worker", endpoint);
        } else {
            endpoint = parentEndpoint;
            hasMailbox = false;
        }
        log.info("Endpoint: " + endpoint);
    }

}