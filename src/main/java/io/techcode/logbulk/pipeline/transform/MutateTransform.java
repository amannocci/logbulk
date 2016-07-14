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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Mutate transformer pipeline component.
 */
public class MutateTransform extends ComponentVerticle {

    @Override public void start() {
        super.start();

        // Aggregate all operations perform
        List<Consumer<JsonObject>> pipeline = Lists.newArrayList();
        if (config.containsKey("remove")) {
            pipeline.add(new RemoveTask(config));
        }
        if (config.containsKey("rename")) {
            pipeline.add(new RenameTask(config));
        }
        if (config.containsKey("strip")) {
            pipeline.add(new StripTask(config));
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

        // Optimize space consumption
        ((ArrayList) pipeline).trimToSize();

        // Register endpoint
        getEventBus().<JsonObject>localConsumer(endpoint)
                .handler((ConvertHandler) msg -> {
                    // Process
                    JsonObject evt = event(msg);
                    pipeline.forEach(t -> t.accept(evt));

                    // Send to the next endpoint
                    forward(msg);
                });
    }

    /**
     * Remove task implementation.
     */
    private class RemoveTask implements Consumer<JsonObject> {

        // Field to remove
        private List<String> toRemove;

        /**
         * Create a new remove task.
         *
         * @param config configuration of the task.
         */
        private RemoveTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toRemove = config.getJsonArray("remove").<String>getList();
            ((ArrayList) toRemove).trimToSize();
        }

        @Override public void accept(JsonObject evt) {
            toRemove.forEach(evt::remove);
        }

    }

    /**
     * Strip task implementation.
     */
    private class StripTask implements Consumer<JsonObject> {

        // Pattern
        private final Pattern pattern = Pattern.compile("\\s+");

        // Field to strip
        private List<String> toStrip;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public StripTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toStrip = config.getJsonArray("strip").<String>getList();
            ((ArrayList) toStrip).trimToSize();
        }

        @Override public void accept(JsonObject evt) {
            toStrip.forEach(key -> {
                if (evt.containsKey(key)) {
                    evt.put(key, pattern.matcher(evt.getString(key)).replaceAll(" "));
                }
            });
        }

    }

    /**
     * Lowercase task implementation.
     */
    private class LowercaseTask implements Consumer<JsonObject> {

        // Field to strip
        private List<String> toLowercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public LowercaseTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toLowercase = config.getJsonArray("lowercase").<String>getList();
            ((ArrayList) toLowercase).trimToSize();
        }

        @Override public void accept(JsonObject evt) {
            toLowercase.forEach(key -> {
                if (evt.containsKey(key)) {
                    evt.put(key, evt.getString(key).toLowerCase());
                }
            });
        }

    }

    /**
     * Uppercase task implementation.
     */
    private class UppercaseTask implements Consumer<JsonObject> {

        // Field to strip
        private List<String> toUppercase;

        /**
         * Create a new strip task.
         *
         * @param config configuration of the task.
         */
        public UppercaseTask(JsonObject config) {
            checkNotNull(config, "The configuration can't be null");
            toUppercase = config.getJsonArray("uppercase").<String>getList();
            ((ArrayList) toUppercase).trimToSize();
        }

        @Override public void accept(JsonObject evt) {
            toUppercase.forEach(key -> {
                if (evt.containsKey(key)) {
                    evt.put(key, evt.getString(key).toUpperCase());
                }
            });
        }

    }

    /**
     * Update task implementation.
     */
    private class UpdateTask implements Consumer<JsonObject> {

        // Element to update
        private Map<String, String> toUpdate;

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

        @Override public void accept(JsonObject evt) {
            toUpdate.keySet().stream().filter(evt::containsKey).forEach(key -> {
                evt.put(key, toUpdate.get(key));
            });
        }

    }

    /**
     * Rename task implementation.
     */
    private class RenameTask implements Consumer<JsonObject> {

        // Element to rename
        private Map<String, String> toRename;

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

        @Override public void accept(JsonObject evt) {
            toRename.keySet().stream().filter(evt::containsKey).forEach(key -> {
                evt.getMap().put(toRename.get(key), evt.getMap().get(key));
                evt.remove(key);
            });
        }

    }

    /**
     * Convert task implementation.
     */
    private class ConvertTask implements Consumer<JsonObject> {

        // Element to convert
        private Map<String, Byte> toConvert;

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

        @Override public void accept(JsonObject evt) {
            toConvert.keySet().stream().filter(evt::containsKey).forEach(key -> {
                switch (toConvert.get(key)) {
                    case 0:
                        Integer intVal = Ints.tryParse(String.valueOf(evt.getMap().get(key)));
                        evt.put(key, intVal == null ? 0 : intVal);
                        break;
                    case 1:
                        evt.put(key, String.valueOf(evt.getMap().get(key)));
                        break;
                    case 2:
                        Float floatVal = Floats.tryParse(String.valueOf(evt.getMap().get(key)));
                        evt.put(key, floatVal == null ? Float.NaN : floatVal);
                        break;
                }
            });
        }

    }

}