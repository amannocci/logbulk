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
package io.techcode.logbulk.io;

import com.google.common.base.Objects;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wrap json configuration to allow proper java properties overrides.
 */
public class Configuration extends JsonObject {

    // Wrap configuration
    private JsonObject wrapConfig;

    /**
     * Create a new configuration.
     *
     * @param config configuration to wrap.
     */
    public Configuration(JsonObject config) {
        this.wrapConfig = checkNotNull(config, "The configuration can't be null");
    }

    /**
     * Create an instance from a string of JSON.
     *
     * @param json the string of JSON.
     */
    public Configuration(String json) {
        wrapConfig = new JsonObject(json);
    }

    /**
     * Create a new, empty instance.
     */
    public Configuration() {
        wrapConfig = new JsonObject();
    }

    /**
     * Create an instance from a Map. The Map is not copied.
     *
     * @param map the map to create the instance from.
     */
    public Configuration(Map<String, Object> map) {
        wrapConfig = new JsonObject(map);
    }

    @Override public String getString(String key) {
        return wrapConfig.getString(key);
    }

    @Override public JsonObject getJsonObject(String key) {
        return wrapConfig.getJsonObject(key);
    }

    @Override public JsonArray getJsonArray(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            JsonArray parse = new JsonArray((String) value);
            wrapConfig.put(key, parse);
            return parse;
        }
        return wrapConfig.getJsonArray(key);
    }

    @Override public byte[] getBinary(String key) {
        return wrapConfig.getBinary(key);
    }

    @Override public Instant getInstant(String key) {
        return wrapConfig.getInstant(key);
    }

    @Override public Object getValue(String key) {
        return wrapConfig.getValue(key);
    }

    @Override public String getString(String key, String def) {
        return wrapConfig.getString(key, def);
    }

    @Override public JsonObject getJsonObject(String key, JsonObject def) {
        return wrapConfig.getJsonObject(key, def);
    }

    @Override public JsonArray getJsonArray(String key, JsonArray def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            JsonArray parse = new JsonArray((String) value);
            wrapConfig.put(key, parse);
            return parse;
        }
        return wrapConfig.getJsonArray(key, def);
    }

    @Override public byte[] getBinary(String key, byte[] def) {
        return wrapConfig.getBinary(key, def);
    }

    @Override public Instant getInstant(String key, Instant def) {
        return wrapConfig.getInstant(key, def);
    }

    @Override public Object getValue(String key, Object def) {
        return wrapConfig.getValue(key, def);
    }

    @Override public boolean containsKey(String key) {
        return wrapConfig.containsKey(key);
    }

    @Override public Set<String> fieldNames() {
        return wrapConfig.fieldNames();
    }

    @Override public JsonObject put(String key, Enum value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, CharSequence value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, String value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Integer value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Long value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Double value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Float value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Boolean value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject putNull(String key) {
        return wrapConfig.putNull(key);
    }

    @Override public JsonObject put(String key, JsonObject value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, JsonArray value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, byte[] value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Instant value) {
        return wrapConfig.put(key, value);
    }

    @Override public JsonObject put(String key, Object value) {
        return wrapConfig.put(key, value);
    }

    @Override public Object remove(String key) {
        return wrapConfig.remove(key);
    }

    @Override public JsonObject mergeIn(JsonObject other) {
        return wrapConfig.mergeIn(other);
    }

    @Override public String encode() {
        return wrapConfig.encode();
    }

    @Override public String encodePrettily() {
        return wrapConfig.encodePrettily();
    }

    @Override public JsonObject copy() {
        return wrapConfig.copy();
    }

    @Override public Map<String, Object> getMap() {
        return wrapConfig.getMap();
    }

    @Override public Stream<Map.Entry<String, Object>> stream() {
        return wrapConfig.stream();
    }

    @Override public Iterator<Map.Entry<String, Object>> iterator() {
        return wrapConfig.iterator();
    }

    @Override public int size() {
        return wrapConfig.size();
    }

    @Override public JsonObject clear() {
        return wrapConfig.clear();
    }

    @Override public boolean isEmpty() {
        return wrapConfig.isEmpty();
    }

    @Override public String toString() {
        return wrapConfig.toString();
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        Configuration entries = (Configuration) o;
        return Objects.equal(wrapConfig, entries.wrapConfig);
    }

    @Override public int hashCode() {
        return wrapConfig.hashCode();
    }

    @Override public void writeToBuffer(Buffer buffer) {
        wrapConfig.writeToBuffer(buffer);
    }

    @Override public int readFromBuffer(int pos, Buffer buffer) {
        return wrapConfig.readFromBuffer(pos, buffer);
    }

    @Override public void forEach(Consumer<? super Map.Entry<String, Object>> action) {
        wrapConfig.forEach(action);
    }

    @Override public Spliterator<Map.Entry<String, Object>> spliterator() {
        return wrapConfig.spliterator();
    }

    @Override public Integer getInteger(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Integer parse = Ints.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getInteger(key);
    }

    @Override public Long getLong(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Long parse = Longs.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getLong(key);
    }

    @Override public Double getDouble(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Double parse = Doubles.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getDouble(key);
    }

    @Override public Float getFloat(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Float parse = Floats.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getFloat(key);
    }

    @Override public Boolean getBoolean(String key) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Boolean parse = Boolean.valueOf((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getBoolean(key);
    }

    @Override public Integer getInteger(String key, Integer def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Integer parse = Ints.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getInteger(key, def);
    }

    @Override public Long getLong(String key, Long def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Long parse = Longs.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getLong(key, def);
    }

    @Override public Double getDouble(String key, Double def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Double parse = Doubles.tryParse((String) value);
            if (parse != null)
                put(key, parse);
            return parse;
        }
        return wrapConfig.getDouble(key, def);
    }

    @Override public Float getFloat(String key, Float def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Float parse = Floats.tryParse((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getFloat(key, def);
    }

    @Override public Boolean getBoolean(String key, Boolean def) {
        Object value = wrapConfig.getValue(key);
        if (value instanceof String) {
            Boolean parse = Boolean.valueOf((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return wrapConfig.getBoolean(key, def);
    }

}