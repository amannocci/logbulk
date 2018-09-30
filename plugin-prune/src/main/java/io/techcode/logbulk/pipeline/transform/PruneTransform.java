/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Mutate transformer pipeline component.
 */
public class PruneTransform extends BaseComponentVerticle {

    // Pipeline
    private List<Consumer<Packet>> pipeline;

    @Override public void start() {
        super.start();

        // Aggregate all operations perform
        pipeline = Lists.newArrayList();
        if (config.containsKey("rename")) {
            pipeline.add(new RenameTask(config));
        }
        if (config.containsKey("strip")) {
            pipeline.add(new StripTask(config));
        }
        if (config.containsKey("join")) {
            pipeline.add(new JoinTask(config));
        }
        if (config.containsKey("uppercase")) {
            pipeline.add(new WhitelistTask(config));
        }
        if (config.containsKey("lowercase")) {
            pipeline.add(new LowercaseTask(config));
        }
        if (config.containsKey("update")) {
            pipeline.add(new UpdateTask(config));
        }
        if (config.containsKey("convert")) {
            pipeline.add(new ConvertTask(config));
        }
        if (config.containsKey("gsub")) {
            pipeline.add(new GsubTask(config));
        }
        if (config.containsKey("mask")) {
            pipeline.add(new MaskTask(config));
        }
        if (config.containsKey("unmask")) {
            pipeline.add(new UnmaskTask(config));
        }
        if (config.containsKey("remove")) {
            pipeline.add(new RemoveTask(config));
        }

        // Optimize space consumption
        ((ArrayList) pipeline).trimToSize();

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        pipeline.forEach(t -> t.accept(packet));

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    /**
     * Whitelist task implementation.
     */
    private class WhitelistTask implements Consumer<Packet> {

        // Field to set
        private Set<String> whitelist;

        /**
         * Create a new whitelist task.
         *
         * @param config configuration of the task.
         * @ @param key key involved.
         */
        public WhitelistTask(@NonNull JsonObject config, String key) {
            Object value = config.getValue(key);
            if (value instanceof Iterable) {
                whitelist = Streams.to(config.getJsonArray(key).stream(), String.class).collect(Collectors.toSet());
            } else {
                Buffer buf = vertx.fileSystem().readFileBlocking(config.getString(key));
                whitelist = Sets.newHashSet(Splitter.on("\n").omitEmptyStrings().trimResults().split(buf.toString()));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            whitelist.forEach(key -> {
                if (body.containsKey(key)) {
                    body.put(key, body.getString(key).toUpperCase());
                }
            });
        }

    }

}