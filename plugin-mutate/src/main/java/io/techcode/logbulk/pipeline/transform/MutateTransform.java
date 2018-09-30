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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
        if (config.containsKey("concat")) {
            pipeline.add(new ConcatTask(config));
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
        if (config.containsKey("split")) {
            pipeline.add(new SplitTask(config));
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
        private MaskTask(@NonNull JsonObject config) {
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
         * @param ignored configuration of the task.
         */
        private UnmaskTask(JsonObject ignored) {
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
        private final List<JsonPath> toRemove;

        /**
         * Create a new remove task.
         *
         * @param config configuration of the task.
         */
        private RemoveTask(@NonNull JsonObject config) {
            toRemove = Streams.to(config.getJsonArray("remove").stream(), String.class)
                    .map(JsonPath::create)
                    .collect(Collectors.toList());
            ((ArrayList) toRemove).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toRemove.forEach(path -> path.remove(body));
        }

    }

    /**
     * Strip task implementation.
     */
    private class StripTask implements Consumer<Packet> {

        // Pattern
        private final Pattern pattern = Pattern.compile("\\s+");

        // Field to strip
        private final List<JsonPath> toStrip;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        private StripTask(@NonNull JsonObject config) {
            toStrip = Streams.to(config.getJsonArray("strip").stream(), String.class)
                    .map(JsonPath::create)
                    .collect(Collectors.toList());
            ((ArrayList) toStrip).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toStrip.forEach(key -> {
                String value = key.get(body, String.class);
                if (value != null) {
                    key.put(body, pattern.matcher(value).replaceAll(" "));
                }
            });
        }

    }

    /**
     * Lowercase task implementation.
     */
    private class LowercaseTask implements Consumer<Packet> {

        // Field to lowercase
        private final List<JsonPath> toLowercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        private LowercaseTask(@NonNull JsonObject config) {
            toLowercase = Streams.to(config.getJsonArray("lowercase").stream(), String.class)
                    .map(JsonPath::create)
                    .collect(Collectors.toList());
            ((ArrayList) toLowercase).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toLowercase.forEach(key -> {
                String value = key.get(body, String.class);
                if (value != null) {
                    key.put(body, value.toLowerCase());
                }
            });
        }

    }

    /**
     * Uppercase task implementation.
     */
    private class UppercaseTask implements Consumer<Packet> {

        // Field to uppercase
        private final List<JsonPath> toUppercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        private UppercaseTask(@NonNull JsonObject config) {
            toUppercase = Streams.to(config.getJsonArray("uppercase").stream(), String.class)
                    .map(JsonPath::create)
                    .collect(Collectors.toList());
            ((ArrayList) toUppercase).trimToSize();
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toUppercase.forEach(key -> {
                String value = key.get(body, String.class);
                if (value != null) {
                    key.put(body, value.toUpperCase());
                }
            });
        }

    }

    /**
     * Concat task implementation.
     */
    private class ConcatTask implements Consumer<Packet> {

        // Element to concat
        private final List<JsonPath> toConcat = Lists.newArrayList();
        private JsonPath target;

        /**
         * Create a new concat task.
         *
         * @param config configuration of the task.
         */
        private ConcatTask(@NonNull JsonObject config) {
            JsonObject concatConf = config.getJsonObject("concat");
            Streams.to(concatConf.getJsonArray("sources", new JsonArray()).stream(), String.class)
                    .map(JsonPath::create)
                    .forEach(toConcat::add);
            target = JsonPath.create(concatConf.getString("target"));
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            StringBuilder builder = new StringBuilder();
            toConcat.forEach(key -> {
                Object value = key.get(body);
                if (value == body) {
                    builder.append(body.encode());
                } else if (value != null) {
                    builder.append(value);
                } else {
                    builder.append(key);
                }
            });
            target.put(body, builder.toString());
        }
    }

    /**
     * Split task implementation.
     */
    private class SplitTask implements Consumer<Packet> {

        // All splitters
        private final Map<JsonPath, Splitter> splitters = Maps.newTreeMap();

        // Element to columns
        private final Table<String, Integer, String> toColumns = HashBasedTable.create();

        /**
         * Create a new split task.
         *
         * @param config configuration of the task.
         */
        private SplitTask(@NonNull JsonObject config) {
            JsonObject splitConf = config.getJsonObject("split");
            for (String field : splitConf.fieldNames()) {
                JsonObject conf = splitConf.getJsonObject(field);
                JsonObject rawColumns = conf.getJsonObject("columns");
                for (String key : rawColumns.fieldNames()) {
                    Integer conv = Ints.tryParse(key);
                    if (conv != null) toColumns.put(field, conv, rawColumns.getString(key));
                }

                // Setup splitter
                Splitter splitter = Splitter.on(conf.getString("delimiter"));
                if (conf.getBoolean("trim", true)) {
                    splitter = splitter.trimResults();
                }
                int limit = conf.getInteger("limit", 0);
                if (limit > 0) {
                    splitter = splitter.limit(limit);
                }
                splitters.put(JsonPath.create(field), splitter);
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            for (Map.Entry<JsonPath, Splitter> el : splitters.entrySet()) {
                String value = el.getKey().get(body, String.class);
                if (!Strings.isNullOrEmpty(value)) {
                    // Split to values
                    List<String> splitted = el.getValue().splitToList(value);

                    // Retrieve row
                    Map<Integer, String> row = toColumns.row(el.getKey().toString());

                    // Iterate over each entry
                    for (Map.Entry<Integer, String> entry : row.entrySet()) {
                        if (entry.getKey() < splitted.size()) {
                            String splitValue = splitted.get(entry.getKey());

                            // We need to replace old value in any case
                            if (Strings.isNullOrEmpty(splitValue)) {
                                body.remove(entry.getValue());
                            } else {
                                body.put(entry.getValue(), splitValue);
                            }
                        }
                    }
                }
            }
        }

    }

    /**
     * Update task implementation.
     */
    private class UpdateTask implements Consumer<Packet> {

        // Element to update
        private final Map<JsonPath, Object> toUpdate = Maps.newTreeMap();

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private UpdateTask(@NonNull JsonObject config) {
            for (Map.Entry<String, Object> entry : config.getJsonObject("update")) {
                toUpdate.put(JsonPath.create(entry.getKey()), entry.getValue());
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toUpdate.keySet().forEach(key -> key.put(body, toUpdate.get(key)));
        }

    }

    /**
     * Gsub task implementation.
     */
    private class GsubTask implements Consumer<Packet> {

        // Element to gsub
        private final Map<JsonPath, Pair<String, String>> toGsub = Maps.newTreeMap();

        /**
         * Create a new gsub task.
         *
         * @param config configuration of the task.
         */
        private GsubTask(@NonNull JsonObject config) {
            for (Map.Entry<String, Object> entry : config.getJsonObject("gsub")) {
                if (entry.getValue() instanceof JsonArray) {
                    JsonArray list = (JsonArray) entry.getValue();
                    if (list.size() == 2) {
                        toGsub.put(JsonPath.create(entry.getKey()), Pair.of(list.getString(0), list.getString(1)));
                    } else {
                        log.error("GSub incorrect values: " + entry.getValue());
                    }
                }
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toGsub.keySet().forEach(key -> {
                String value = key.get(body, String.class);
                if (value != null) {
                    Pair<String, String> pair = toGsub.get(key);
                    key.put(body, value.replaceAll(pair.getKey(), pair.getValue()));
                }
            });
        }

    }

    /**
     * Join task implementation.
     */
    private class JoinTask implements Consumer<Packet> {

        // Element to join
        private final Map<JsonPath, String> toJoin = Maps.newTreeMap();

        /**
         * Create a new join task.
         *
         * @param config configuration of the task.
         */
        private JoinTask(@NonNull JsonObject config) {
            for (Map.Entry<String, Object> entry : config.getJsonObject("join")) {
                toJoin.put(JsonPath.create(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toJoin.keySet().forEach(key -> {
                List list = key.get(body, List.class);
                if (list != null) {
                    String joined = Joiner.on(toJoin.get(key)).join(list);
                    key.put(body, joined);
                }
            });
        }

    }

    /**
     * Rename task implementation.
     */
    private class RenameTask implements Consumer<Packet> {

        // Element to rename
        private final Map<JsonPath, JsonPath> toRename = Maps.newTreeMap();

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private RenameTask(@NonNull JsonObject config) {
            for (Map.Entry<String, Object> entry : config.getJsonObject("rename")) {
                toRename.put(JsonPath.create(entry.getKey()), JsonPath.create(String.valueOf(entry.getValue())));
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toRename.keySet().forEach(key -> {
                toRename.get(key).put(body, key.get(body));
                key.remove(body);
            });
        }

    }

    /**
     * Convert task implementation.
     */
    private class ConvertTask implements Consumer<Packet> {

        // Element to convert
        private final Map<JsonPath, Byte> toConvert = Maps.newTreeMap();

        /**
         * Create a new remame task.
         *
         * @param config configuration of the task.
         */
        private ConvertTask(@NonNull JsonObject config) {
            for (Map.Entry<String, Object> entry : config.getJsonObject("convert")) {
                switch (String.valueOf(entry.getValue())) {
                    case "integer":
                        toConvert.put(JsonPath.create(entry.getKey()), (byte) 0);
                        break;
                    case "string":
                        toConvert.put(JsonPath.create(entry.getKey()), (byte) 1);
                        break;
                    case "float":
                        toConvert.put(JsonPath.create(entry.getKey()), (byte) 2);
                        break;
                }
            }
        }

        @Override public void accept(Packet packet) {
            JsonObject body = packet.getBody();
            toConvert.keySet().forEach(key -> {
                switch (toConvert.get(key)) {
                    case 0:
                        Integer intVal = Ints.tryParse(key.get(body, String.class));
                        key.put(body, intVal == null ? 0 : intVal);
                        break;
                    case 1:
                        key.put(body, String.valueOf(key.get(body)));
                        break;
                    case 2:
                        Float floatVal = Floats.tryParse(key.get(body, String.class));
                        key.put(body, floatVal == null ? Float.NaN : floatVal);
                        break;
                }
            });
        }

    }

}