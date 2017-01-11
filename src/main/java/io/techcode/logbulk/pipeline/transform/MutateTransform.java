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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Mutate transformer pipeline component.
 */
public class MutateTransform extends BaseComponentVerticle {

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
            pipeline.add(new UppercaseTask(config));
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
     * Mask task implementation.
     */
    private class MaskTask implements Consumer<Packet> {

        // Field to mask
        private final String toMask;

        /**
         * Create a new mask task.
         *
         * @param config configuration of the task.
         */
        private MaskTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toMask = config.getString("mask");
        }

        @Override public void accept(Packet packet) {
            Packet.Header headers = packet.getHeader();
            JsonObject body = packet.getBody();

            if (body.containsKey(toMask) && body.getValue(toMask) instanceof JsonObject) {
                JsonObject mask = body.getJsonObject(toMask);
                headers.put("_mask", body);
                packet.setBody(mask);
            }
        }

    }

    /**
     * Unmask task implementation.
     */
    private class UnmaskTask implements Consumer<Packet> {

        /**
         * Create a new unmask task.
         *
         * @param config configuration of the task.
         */
        private UnmaskTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
        }

        @Override public void accept(Packet packet) {
            Packet.Header headers = packet.getHeader();

            if (headers.containsKey("_mask")) {
                JsonObject unmask = headers.getJsonObject("_mask");
                headers.remove("_mask");
                packet.setBody(unmask);
            }
        }

    }

    /**
     * Remove task implementation.
     */
    private class RemoveTask implements Consumer<Packet> {

        // Field to remove
        private final List<String> toRemove;

        /**
         * Create a new remove task.
         *
         * @param config configuration of the task.
         */
        private RemoveTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toRemove = Streams.to(config.getJsonArray("remove").stream(), String.class).collect(Collectors.toList());
            ((ArrayList) toRemove).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toRemove.forEach(body::remove);
        }

    }

    /**
     * Strip task implementation.
     */
    private class StripTask implements Consumer<Packet> {

        // Pattern
        private final Pattern pattern = Pattern.compile("\\s+");

        // Field to strip
        private final List<String> toStrip;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public StripTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toStrip = Streams.to(config.getJsonArray("strip").stream(), String.class).collect(Collectors.toList());
            ((ArrayList) toStrip).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toStrip.forEach(key -> {
                if (body.containsKey(key)) {
                    body.put(key, pattern.matcher(body.getString(key)).replaceAll(" "));
                }
            });
        }

    }

    /**
     * Lowercase task implementation.
     */
    private class LowercaseTask implements Consumer<Packet> {

        // Field to strip
        private final List<String> toLowercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public LowercaseTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toLowercase = Streams.to(config.getJsonArray("lowercase").stream(), String.class).collect(Collectors.toList());
            ((ArrayList) toLowercase).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toLowercase.forEach(key -> {
                if (body.containsKey(key)) {
                    body.put(key, body.getString(key).toLowerCase());
                }
            });
        }

    }

    /**
     * Uppercase task implementation.
     */
    private class UppercaseTask implements Consumer<Packet> {

        // Field to strip
        private final List<String> toUppercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public UppercaseTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toUppercase = Streams.to(config.getJsonArray("uppercase").stream(), String.class).collect(Collectors.toList());
            ((ArrayList) toUppercase).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toUppercase.forEach(key -> {
                if (body.containsKey(key)) {
                    body.put(key, body.getString(key).toUpperCase());
                }
            });
        }

    }

    /**
     * Update task implementation.
     */
    private class UpdateTask implements Consumer<Packet> {

        // Element to update
        private final Map<String, String> toUpdate;

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private UpdateTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toUpdate = Maps.newTreeMap();
            for (Map.Entry<String, Object> entry : config.getJsonObject("update")) {
                toUpdate.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toUpdate.keySet().stream().filter(body::containsKey).forEach(key -> {
                body.put(key, toUpdate.get(key));
            });
        }

    }

    /**
     * Gsub task implementation.
     */
    private class GsubTask implements Consumer<Packet> {

        // Element to gsub
        private final Map<String, Pair<String, String>> toGsub = Maps.newTreeMap();

        /**
         * Create a new gsub task.
         *
         * @param config configuration of the task.
         */
        private GsubTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            for (Map.Entry<String, Object> entry : config.getJsonObject("gsub")) {
                if (entry.getValue() instanceof JsonArray) {
                    JsonArray list = (JsonArray) entry.getValue();
                    if (list.size() == 2) {
                        toGsub.put(entry.getKey(), Pair.of(list.getString(0), list.getString(1)));
                    } else {
                        log.error("GSub incorrect values: " + entry.getValue());
                    }
                }
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toGsub.keySet().stream().filter(body::containsKey).forEach(key -> {
                if (body.getValue(key) instanceof String) {
                    Pair<String, String> pair = toGsub.get(key);
                    body.put(key, body.getString(key).replaceAll(pair.getKey(), pair.getValue()));
                }
            });
        }

    }

    /**
     * Join task implementation.
     */
    private class JoinTask implements Consumer<Packet> {

        // Element to join
        private final Map<String, String> toJoin;

        /**
         * Create a new join task.
         *
         * @param config configuration of the task.
         */
        private JoinTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toJoin = Maps.newTreeMap();
            for (Map.Entry<String, Object> entry : config.getJsonObject("join")) {
                toJoin.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toJoin.keySet().stream().filter(body::containsKey).forEach(key -> {
                Object raw = body.getMap().get(key);
                if (raw instanceof List) {
                    List list = (List) raw;
                    String joined = Joiner.on(toJoin.get(key)).join(list);
                    body.put(key, joined);
                }
            });
        }

    }

    /**
     * Rename task implementation.
     */
    private class RenameTask implements Consumer<Packet> {

        // Element to rename
        private final Map<String, String> toRename;

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private RenameTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toRename = Maps.newTreeMap();
            for (Map.Entry<String, Object> entry : config.getJsonObject("rename")) {
                toRename.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toRename.keySet().stream().filter(body::containsKey).forEach(key -> {
                body.getMap().put(toRename.get(key), body.getMap().get(key));
                body.remove(key);
            });
        }

    }

    /**
     * Convert task implementation.
     */
    private class ConvertTask implements Consumer<Packet> {

        // Element to convert
        private final Map<String, Byte> toConvert;

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private ConvertTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toConvert = Maps.newTreeMap();
            for (Map.Entry<String, Object> entry : config.getJsonObject("convert")) {
                switch (String.valueOf(entry.getValue())) {
                    case "integer":
                        toConvert.put(entry.getKey(), (byte) 0);
                        break;
                    case "string":
                        toConvert.put(entry.getKey(), (byte) 1);
                        break;
                    case "float":
                        toConvert.put(entry.getKey(), (byte) 2);
                        break;
                }
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toConvert.keySet().stream().filter(body::containsKey).forEach(key -> {
                switch (toConvert.get(key)) {
                    case 0:
                        Integer intVal = Ints.tryParse(String.valueOf(body.getMap().get(key)));
                        body.put(key, intVal == null ? 0 : intVal);
                        break;
                    case 1:
                        body.put(key, String.valueOf(body.getMap().get(key)));
                        break;
                    case 2:
                        Float floatVal = Floats.tryParse(String.valueOf(body.getMap().get(key)));
                        body.put(key, floatVal == null ? Float.NaN : floatVal);
                        break;
                }
            });
        }

    }

}