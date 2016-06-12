/**
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
import com.google.common.primitives.Ints;
import io.techcode.logbulk.util.PressureHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.CaseInsensitiveHeaders;
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
     * Returns the previous component processor for this event.
     *
     * @param headers headers of the event.
     * @return previous component processor for this event.
     */
    public Optional<String> previous(MultiMap headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        Integer currentRaw = Ints.tryParse(headers.get("_current"));
        int current = currentRaw != null ? currentRaw : 0;
        int previous = current - 1;

        // Possible previous component
        if (previous >= 0) {
            String route = headers.get("_route");
            List<String> routing = this.routing.get(route);
            return Optional.of(routing.get(previous));
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
    public Optional<String> next(MultiMap headers) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        Integer currentRaw = Ints.tryParse(headers.get("_current"));
        int current = currentRaw != null ? currentRaw : 0;
        return next(headers, current);
    }

    /**
     * Returns the next component processor for this event.
     *
     * @param headers headers of the event.
     * @return next component processor for this event.
     */
    private Optional<String> next(MultiMap headers, int current) {
        checkNotNull(headers, "Headers can't be null");

        // Gets some stuff
        int next = current + 1;

        // Retrieve routing
        String route = headers.get("_route");
        List<String> routing = this.routing.get(route);

        // Possible next component
        if (next < routing.size()) {
            return Optional.of(routing.get(next));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Forward the event to the next stage.
     *
     * @param headers headers of the event.
     * @param evt     event to forward.
     */
    public void forward(MultiMap headers, JsonObject evt) {
        checkNotNull(headers, "Headers can't be null");
        checkNotNull(evt, "The event to forward can't be null");

        // Gets some stuff
        Integer currentRaw = Ints.tryParse(headers.get("_current"));
        int current = currentRaw != null ? currentRaw : 0;

        // Determine next stage
        Optional<String> nextOpt = next(headers, current);
        if (nextOpt.isPresent()) {
            headers.set("_current", String.valueOf(current + 1));
            eventBus.send(nextOpt.get(), evt, new DeliveryOptions().setCodecName("fastjsonobject").setHeaders(headers));
        }
        eventBus.send(parentEndpoint + ".worker", endpoint);
    }

    /**
     * Handle back-pressure on component.
     *
     * @param stream stream in read.
     * @param config source configuration.
     */
    public void handlePressure(ReadStream stream, JsonObject config) {
        eventBus.<String>consumer(config.getString("endpoint") + ".pressure")
                .handler(new PressureHandler(stream));
    }

    /**
     * Returns a new input parser.
     *
     * @param config configuration details.
     * @return new input parser.
     */
    public RecordParser inputParser(JsonObject config) {
        return RecordParser.newDelimited(config.getString("delimiter", "\n"), buf -> {
            // Create a new event
            MultiMap headers = new CaseInsensitiveHeaders();
            JsonObject evt = new JsonObject().put("message", buf.toString());

            // Options
            headers.add("_route", config.getString("dispatch"));
            headers.add("_current", "0");

            // Send to the next endpoint
            forward(headers, evt);
        });
    }

    /**
     * Notify pressure to another component.
     *
     * @param previousPressure already notified component.
     * @param headers          headers event involved.
     */
    public void notifyPressure(Set<String> previousPressure, MultiMap headers) {
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

        if (config.getBoolean("origin", false)) {
            endpoint = parentEndpoint;
        } else {
            endpoint = parentEndpoint + ".worker." + uuid;
            eventBus.send(parentEndpoint + ".worker", endpoint);
        }
        log.info("Endpoint: " + endpoint);
    }

}