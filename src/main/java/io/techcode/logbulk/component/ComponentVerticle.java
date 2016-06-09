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

import com.google.common.primitives.Ints;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.streams.ReadStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

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
     * @param headers headers of the event.
     * @param evt     event to forward.
     */
    public void forward(MultiMap headers, JsonObject evt) {
        // Check arguments
        checkNotNull(headers, "Headers can't be null");
        checkNotNull(evt, "The event to forward can't be null");

        // Gets some stuff
        Integer currentRaw = Ints.tryParse(headers.get("_current"));
        int current = currentRaw != null ? currentRaw : 0;
        List<String> routing = headers.getAll("_route");

        // Determine next stage
        if (current < routing.size()) {
            headers.set("_current", String.valueOf(current + 1));
            vertx.eventBus().send(routing.get(current), evt, new DeliveryOptions().setCodecName("fastjsonobject").setHeaders(headers));
        }
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
     * @param headers headers of the event to analyze.
     * @return route source of the event.
     */
    public String source(MultiMap headers) {
        return "route." + headers.get("_source");
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
            headers.add("_route", (Iterable<String>) config.getJsonObject("route").getJsonArray(config.getString("dispatch")).getList());
            headers.add("_current", String.valueOf(0));
            headers.add("_source", config.getString("source"));

            // Send to the next endpoint
            forward(headers, evt);
        });
    }

}