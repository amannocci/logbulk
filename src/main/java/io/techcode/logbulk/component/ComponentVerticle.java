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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * Component verticle helper.
 */
@Slf4j
public class ComponentVerticle extends AbstractVerticle {

    // UUID of the component
    @Getter private final String uuid = UUID.randomUUID().toString();

    /**
     * Forward the event to the next stage.
     *
     * @param evt event to forward.
     */
    public void forward(JsonObject evt) {
        // Gets some stuff
        int current = evt.getInteger("_current");
        JsonArray routing = evt.getJsonArray("_route");

        // Determine next stage
        if (current < routing.size()) {
            evt.put("_current", current + 1);
            vertx.eventBus().send(routing.getString(current), evt, new DeliveryOptions().setCodecName("fastjsonobject"));
        }
    }

    /**
     * Mask all internal fields.
     *
     * @param evt event to process.
     * @return new masked event.
     */
    public JsonObject mask(JsonObject evt) {
        JsonObject clone = evt.copy();
        clone.remove("_route");
        clone.remove("_current");
        clone.remove("_source");
        clone.remove("_index");
        return clone;
    }

    /**
     * Demand a source pause.
     *
     * @param source   source to pause.
     * @param endpoint endpoint demand.
     */
    public void pause(String source, String endpoint) {
        vertx.eventBus().publish(source + ".backpressure", new JsonObject()
                .put("action", "pause")
                .put("endpoint", endpoint));
    }

    /**
     * Demand a source resume.
     *
     * @param source   source to resume.
     * @param endpoint endpoint demand.
     */
    public void resume(String source, String endpoint) {
        vertx.eventBus().publish(source + ".backpressure", new JsonObject()
                .put("action", "resume")
                .put("endpoint", endpoint));
    }

    /**
     * Handle back-pressure on component.
     *
     * @param stream stream in read.
     * @param config source configuration.
     */
    public void handleBackPressure(ReadStream stream, JsonObject config) {
        vertx.eventBus().<JsonObject>consumer("route." + config.getString("source") + ".backpressure")
                .handler(new Sourcebox(stream));
    }

    /**
     * Returns the route source of the event.
     *
     * @param evt event to analyze.
     * @return route source of the event.
     */
    public String source(JsonObject evt) {
        return "route." + evt.getString("_source");
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
            JsonObject evt = new JsonObject().put("message", buf.toString());
            evt.put("_route", config.getJsonObject("route").getJsonArray(config.getString("dispatch")));
            evt.put("_current", 0);
            evt.put("_source", config.getString("source"));

            // Send to the next endpoint
            forward(evt);
        });
    }

}