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
package io.techcode.logbulk.io;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Wrap json configuration to allow proper java properties overrides.
 */
public class Configuration extends JsonObject {

    /**
     * Create a new configuration.
     *
     * @param config configuration to wrap.
     */
    public Configuration(JsonObject config) {
        super(config.getMap());
    }

    /**
     * Create an instance from a string of JSON.
     *
     * @param json the string of JSON.
     */
    public Configuration(String json) {
        super(new JsonObject(json).getMap());
    }

    /**
     * Create a new, empty instance.
     */
    public Configuration() {
        super();
    }

    /**
     * Create an instance from a Map. The Map is not copied.
     *
     * @param map the map to create the instance from.
     */
    public Configuration(Map<String, Object> map) {
        // Warning : We can inject null map which generate an invalid json object state
        super(map == null ? new LinkedHashMap<>() : map);
    }

    private <T> T getTyped(String key, Function<String, T> func, Supplier<T> supplier) {
        Object value = super.getValue(key);
        if (value instanceof String) {
            T parse = func.apply((String) value);
            if (parse != null) {
                put(key, parse);
                return parse;
            }
        }
        return supplier.get();
    }

    @Override public Integer getInteger(String key) {
        return getTyped(key, Ints::tryParse, () -> super.getInteger(key));
    }

    @Override public Integer getInteger(String key, Integer def) {
        return getTyped(key, Ints::tryParse, () -> super.getInteger(key, def));
    }

    @Override public Long getLong(String key) {
        return getTyped(key, Longs::tryParse, () -> super.getLong(key));
    }

    @Override public Long getLong(String key, Long def) {
        return getTyped(key, Longs::tryParse, () -> super.getLong(key, def));
    }

    @Override public Double getDouble(String key) {
        return getTyped(key, Doubles::tryParse, () -> super.getDouble(key));
    }

    @Override public Double getDouble(String key, Double def) {
        return getTyped(key, Doubles::tryParse, () -> super.getDouble(key, def));
    }

    @Override public Float getFloat(String key) {
        return getTyped(key, Floats::tryParse, () -> super.getFloat(key));
    }

    @Override public Float getFloat(String key, Float def) {
        return getTyped(key, Floats::tryParse, () -> super.getFloat(key, def));
    }

    @Override public Boolean getBoolean(String key) {
        return getTyped(key, Boolean::valueOf, () -> super.getBoolean(key));
    }

    @Override public Boolean getBoolean(String key, Boolean def) {
        return getTyped(key, Boolean::valueOf, () -> super.getBoolean(key, def));
    }

    @Override public JsonObject getJsonObject(String key) {
        return getTyped(key, JsonObject::new, () -> super.getJsonObject(key));
    }

    @Override public JsonObject getJsonObject(String key, JsonObject def) {
        return getTyped(key, JsonObject::new, () -> super.getJsonObject(key, def));
    }

    @Override public JsonArray getJsonArray(String key) {
        return getTyped(key, JsonArray::new, () -> super.getJsonArray(key));
    }

    @Override public JsonArray getJsonArray(String key, JsonArray def) {
        return getTyped(key, JsonArray::new, () -> super.getJsonArray(key, def));
    }

}
