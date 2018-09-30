/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2017
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.techcode.logbulk.pipeline.transform;

import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.json.JsonObject;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.charset.StandardCharsets;

import static com.google.common.base.Preconditions.checkState;

/**
 * Compress transformer pipeline component.
 */
public class CompressTransform extends BaseComponentVerticle {

    // Some constants
    private static final String CONF_FIELD = "field";
    private static final String CONF_TARGET = "target";
    private static final String CONF_MODE = "mode";

    // Settings
    private JsonPath field;
    private JsonPath target;
    private boolean compress;

    // Depends on mode
    private LZ4Compressor compressor;
    private LZ4FastDecompressor decompressor;

    @Override public void start() {
        super.start();

        // Setup
        field = JsonPath.create(config.getString(CONF_FIELD));
        target = JsonPath.create(config.getString(CONF_TARGET));
        compress = "compress".equals(config.getString(CONF_MODE));
        if (compress) {
            compressor = LZ4Factory.fastestInstance().fastCompressor();
        } else {
            decompressor = LZ4Factory.fastestInstance().fastDecompressor();
        }

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        JsonObject body = packet.getBody();
        if (compress) {
            String value = field.get(body, String.class);
            if (value != null) {
                compress(body, value);
            }
        } else {
            JsonObject value = field.get(body, JsonObject.class);
            if (value != null && value.containsKey("length") && value.containsKey("data")) {
                decompress(body, value);
            }
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    /**
     * Compress a string.
     *
     * @param body  packet body.
     * @param value value to compress.
     */
    private void compress(JsonObject body, String value) {
        byte[] data = value.getBytes(StandardCharsets.UTF_8);
        int decompressedLength = data.length;
        int maxCompressedLength = compressor.maxCompressedLength(decompressedLength);
        byte[] compressed = new byte[maxCompressedLength];
        compressor.compress(data, 0, decompressedLength, compressed, 0, maxCompressedLength);
        target.put(body, new JsonObject().put("length", decompressedLength).put("data", compressed));
    }

    /**
     * Decompress a string.
     *
     * @param body  packet body.
     * @param value value to decompress.
     */
    private void decompress(JsonObject body, JsonObject value) {
        int decompressedLength = value.getInteger("length");
        byte[] restored = new byte[decompressedLength];
        decompressor.decompress(value.getBinary("data"), 0, restored, 0, decompressedLength);
        target.put(body, new String(restored, StandardCharsets.UTF_8));
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString(CONF_FIELD) != null, "The field is required");
        checkState(config.getString(CONF_TARGET) != null, "The target is required");
        checkState(config.getString(CONF_MODE) != null &&
                ("compress".equals(config.getString(CONF_MODE)) || "decompress".equals(config.getString(CONF_MODE))), "The mode is required");
    }

}