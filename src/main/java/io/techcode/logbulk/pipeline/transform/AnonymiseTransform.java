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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.component.Mailbox;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

/**
 * Anonymise transformer pipeline component.
 */
public class AnonymiseTransform extends ComponentVerticle {

    @Override public void start() {
        // Configuration
        JsonObject config = config();

        // Setup
        List<String> fields = config.getJsonArray("fields").getList();
        HashFunction hash;
        switch (config.getString("hashing")) {
            case "sha1":
                hash = Hashing.sha1();
                break;
            case "sha256":
                hash = Hashing.sha256();
                break;
            case "sha384":
                hash = Hashing.sha384();
                break;
            case "sha512":
                hash = Hashing.sha512();
                break;
            case "alder32":
                hash = Hashing.adler32();
                break;
            case "crc32":
                hash = Hashing.crc32();
                break;
            case "crc32c":
                hash = Hashing.crc32c();
                break;
            default:
                hash = Hashing.md5();
                break;
        }
        String endpoint = config.getString("endpoint");

        // Register endpoint
        vertx.eventBus().<JsonObject>consumer(endpoint)
                .handler(new Mailbox(this, endpoint, config.getInteger("mailbox", Mailbox.DEFAULT_THREEHOLD), evt -> {
                    // Process
                    fields.stream().filter(evt::containsKey).forEach(field -> {
                        evt.put(field, hash.hashString(evt.getString(field), StandardCharsets.UTF_8).toString());
                    });

                    // Send to the next endpoint
                    forward(evt);
                }));
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("endpoint") != null, "The endpoint is required");
        checkState(config.getString("hashing") != null, "The hashing is required");
        checkState(config.getJsonArray("fields") != null
                && config.getJsonArray("fields").size() > 0, "The fields is required");
        return config;
    }

}