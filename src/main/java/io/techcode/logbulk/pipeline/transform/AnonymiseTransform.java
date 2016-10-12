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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Anonymise transformer pipeline component.
 */
public class AnonymiseTransform extends ComponentVerticle {

    // Hashing constant
    private static final Map<String, HashFunction> HASHING = new HashMap<String, HashFunction>() {{
        put("sha1", Hashing.sha1());
        put("sha256", Hashing.sha256());
        put("sha384", Hashing.sha384());
        put("sha512", Hashing.sha512());
        put("alder32", Hashing.adler32());
        put("crc32", Hashing.crc32());
        put("crc32c", Hashing.crc32c());
    }};

    @Override public void start() {
        super.start();

        // Setup
        List<String> fields = config.getJsonArray("fields").getList();
        HashFunction hash = HASHING.getOrDefault("hashing", Hashing.md5());

        // Register endpoint
        getEventBus().<JsonObject>localConsumer(endpoint)
                .handler(new TolerantHandler() {
                    @Override public void handle(JsonObject msg) {
                        // Process
                        JsonObject evt = event(msg);
                        fields.stream().filter(evt::containsKey).forEach(field -> {
                            evt.put(field, hash.hashString(evt.getString(field), StandardCharsets.UTF_8).toString());
                        });

                        // Send to the next endpoint
                        forward(msg);
                    }
                });
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("hashing") != null, "The hashing is required");
        checkState(config.getJsonArray("fields") != null
                && config.getJsonArray("fields").size() > 0, "The fields is required");
    }

}