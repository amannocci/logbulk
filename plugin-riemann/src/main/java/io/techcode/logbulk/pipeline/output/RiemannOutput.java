/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2017
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

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.techcode.logbulk.net.Proto;
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;

/**
 * Riemann output pipeline component.
 */
public class RiemannOutput extends TcpOutput {

    // Some constants
    private static final String CONF_BULK = "bulk";
    private static final String CONF_DATA = "data";
    private static final String CONF_HOSTNAME = "hostname";

    // Settings
    private JsonPath hostname;
    private JsonPath data;
    private int bulk;

    @Override public void onStart() {
        // Setup
        bulk = config.getInteger(CONF_BULK, 100);
        hostname = JsonPath.create(config.getString(CONF_HOSTNAME));
        data = JsonPath.create(config.getString(CONF_DATA));
    }

    @Override public Handler<JsonObject> encoder() {
        return new Handler<JsonObject>() {
            Proto.Msg.Builder builder = Proto.Msg.newBuilder();

            @Override public void handle(JsonObject body) {
                String hostValue = hostname.get(body, String.class);
                JsonObject dataValue = data.get(body, JsonObject.class);

                if (!Strings.isNullOrEmpty(hostValue) && dataValue != null) {
                    // Create a new event
                    Proto.Event.Builder event = Proto.Event.newBuilder();
                    event.setTime(System.currentTimeMillis());
                    event.setHost(hostValue);

                    // Map all fields
                    dataValue.stream().forEach(entry -> event.addAttributes(Proto.Attribute.newBuilder()
                            .setKey(entry.getKey())
                            .setValue(String.valueOf(entry.getValue()))
                            .build()));

                    // Map event
                    builder.addEvents(event);

                    // Send
                    if (builder.getEventsList().size() == bulk) {
                        Buffer buf = Buffer.buffer();
                        try {
                            buf.appendInt(0);
                            builder.build().writeTo(new ByteBufOutputStream(buf.getByteBuf()));
                            buf.setInt(0, buf.length() - Ints.BYTES);
                            getConnection().write(buf);
                        } catch (IOException e) {
                            log.error("A message has been discarded", e);
                        }
                        builder = Proto.Msg.newBuilder();

                        // Read every response but currently don't handle them
                        getConnection().resume();
                    }
                }
            }
        };
    }

    @Override protected void checkConfig(JsonObject config) {
        super.checkConfig(config);
        checkState(config.getString(CONF_DATA) != null, "The data is required");
        checkState(config.getString(CONF_HOSTNAME) != null, "The hostname is required");
    }

}
