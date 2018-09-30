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
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBufOutputStream;
import io.techcode.logbulk.util.SyslogHeader;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Syslog output pipeline component.
 */
public class SyslogOutput extends TcpOutput {

    // Some constants
    private static final String CONF_FRAMING = "framing";
    private static final String CONF_MAPPING = "mapping";
    private static final String CONF_DELIMITER = "delimiter";

    // Settings
    private Map<SyslogHeader, String> mapping;
    private String delimiter;

    @Override public void onStart() {
        // Setup mapping
        JsonObject map = config.getJsonObject(CONF_MAPPING, new JsonObject());
        mapping = Maps.newEnumMap(SyslogHeader.class);
        for (String entry : map.fieldNames()) {
            mapping.put(SyslogHeader.byName(entry), map.getString(entry));
        }

        // Setup framing
        String framing = config.getString(CONF_FRAMING);
        if ("delimited".equals(framing)) {
            delimiter = config.getString(CONF_DELIMITER);
        }
    }

    @Override public Handler<JsonObject> encoder() {
        return data -> {
            Buffer buf = Buffer.buffer();

            // Add facility
            buf.appendString("<");
            String facility = mapping.get(SyslogHeader.FACILITY);
            String severity = mapping.get(SyslogHeader.SEVERITY);
            if (mapping.containsKey(SyslogHeader.FACILITY) && mapping.containsKey(SyslogHeader.SEVERITY) && data.containsKey(facility) && data.containsKey(severity)) {
                int acc = data.getInteger(facility) * 8 + data.getInteger(severity);
                buf.appendString(String.valueOf(acc));
            }
            buf.appendString(">1 ");

            // Add timestamp
            populate(buf, SyslogHeader.TIMESTAMP, data);

            // Add hostname
            populate(buf, SyslogHeader.HOST, data);

            // Add application
            populate(buf, SyslogHeader.APPLICATION, data);

            // Add processus
            populate(buf, SyslogHeader.PROCESSUS, data);

            // Add msg id
            populate(buf, SyslogHeader.ID, data);

            // Add structured data
            populate(buf, SyslogHeader.DATA, data);

            // Add message
            String message = mapping.get(SyslogHeader.MESSAGE);
            if (mapping.containsKey(SyslogHeader.MESSAGE)) {
                if (data.containsKey(message)) {
                    buf.appendString(String.valueOf(data.getValue(message)));
                } else {
                    try {
                        Json.mapper.writeValue(new ByteBufOutputStream(buf.getByteBuf()), data.getMap());
                    } catch (Exception e) {
                        throw new EncodeException("Failed to encode as JSON: " + e.getMessage());
                    }
                }
            }

            getConnection().write(framing(buf));
        };
    }

    /**
     * Framing based on buffer.
     *
     * @param buf buffer involved.
     * @return framed buffer.
     */
    private Buffer framing(Buffer buf) {
        if (delimiter != null) {
            return buf.appendString(delimiter);
        } else {
            return Buffer.buffer().appendString(String.valueOf(buf.length())).appendString(" ").appendBuffer(buf);
        }
    }

    /**
     * Populate header based on data.
     *
     * @param buf    buffer to write in.
     * @param header header to populate.
     * @param data   data involved.
     */
    private void populate(Buffer buf, SyslogHeader header, JsonObject data) {
        String key = mapping.get(header);
        if (mapping.containsKey(header) && data.containsKey(key)) {
            buf.appendString(String.valueOf(data.getValue(key)));
            buf.appendString(" ");
        } else {
            buf.appendString("- ");
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        super.checkConfig(config);
        checkState(validFraming(config.getString(CONF_FRAMING)), "The framing is required");
        if ("delimited".equals(config.getString(CONF_FRAMING))) {
            checkState(!Strings.isNullOrEmpty(config.getString(CONF_DELIMITER)), "The delimiter is required");
        }
    }

    /**
     * Validate framing option.
     *
     * @param framing framing option.
     * @return true if the option is valid, otherwise false.
     */
    private boolean validFraming(String framing) {
        return !Strings.isNullOrEmpty(framing) && ("counted".equals(framing) | "delimited".equals(framing));
    }

}
