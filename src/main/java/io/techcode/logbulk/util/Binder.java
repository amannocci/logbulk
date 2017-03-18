/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2016-2017
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
package io.techcode.logbulk.util;

import io.vertx.core.json.JsonObject;
import lombok.NonNull;

/**
 * Binder helper for Json Object binding.
 */
public class Binder<T extends Binder<T>> {

    // Some functions
    public static final Function IDENTITY = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        Object value = dataFrom.getValue(fieldFrom);
        if (value != null) dataTo.put(fieldTo, value);
    };
    public static final Function ANY_TO_STRING = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        Object value = dataFrom.getValue(fieldFrom);
        if (value != null) dataTo.put(fieldTo, String.valueOf(value));
    };
    public static final Function STRING_TO_INT = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        String value = dataFrom.getString(fieldFrom);
        if (value != null) dataTo.put(fieldTo, Integer.parseInt(value));
    };
    public static final Function STRING_TO_LONG = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        String value = dataFrom.getString(fieldFrom);
        if (value != null) dataTo.put(fieldTo, Long.parseLong(value));
    };
    public static final Function STRING_TO_FLOAT = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        String value = dataFrom.getString(fieldFrom);
        if (value != null) dataTo.put(fieldTo, Float.parseFloat(value));
    };
    public static final Function STRING_TO_DOUBLE = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        String value = dataFrom.getString(fieldFrom);
        if (value != null) dataTo.put(fieldTo, Double.parseDouble(value));
    };
    public static final Function STRING_TO_BOOLEAN = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        String value = dataFrom.getString(fieldFrom);
        if (value != null) dataTo.put(fieldTo, Boolean.parseBoolean(value));
    };
    public static final Function INT_TO_BOOLEAN = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        Integer value = dataFrom.getInteger(fieldFrom);
        if (value != null) dataTo.put(fieldTo, value != 0);
    };
    public static final Function LONG_TO_BOOLEAN = (dataFrom, dataTo, fieldFrom, fieldTo) -> {
        Long value = dataFrom.getLong(fieldFrom);
        if (value != null) dataTo.put(fieldTo, value != 0);
    };

    // From object
    private JsonObject from;

    // To object
    private JsonObject to;

    /**
     * Returns same object for chaining.
     *
     * @param from from object.
     * @return same object for chaining.
     */
    @SuppressWarnings("unchecked")
    public T from(@NonNull JsonObject from) {
        this.from = from;
        return (T) this;
    }

    /**
     * Returns same object for chaining.
     *
     * @param to to object.
     * @return same object for chaining.
     */
    @SuppressWarnings("unchecked")
    public T to(@NonNull JsonObject to) {
        this.to = to;
        return (T) this;
    }

    /**
     * Bind a field of from object to.
     *
     * @param from     from field to bind.
     * @param to       to field to bind.
     * @param function transformation to apply
     * @return same object for chaining.
     */
    @SuppressWarnings("unchecked")
    public T bind(String from, String to, Function function) {
        function.apply(this.from, this.to, from, to);
        return (T) this;
    }

    /**
     * Bind a field or null of from object to.
     *
     * @param from     from field to bind.
     * @param to       to field to bind.
     * @param function transformation to apply
     * @return same object for chaining.
     */
    @SuppressWarnings("unchecked")
    public T bindOrNull(String from, String to, Function function) {
        bind(from, to, function);
        if (!this.to.containsKey(to)) {
            this.to.putNull(to);
        }
        return (T) this;
    }

    /**
     * Binder function definition.
     */
    @FunctionalInterface
    public interface Function {

        /**
         * Applies this function to the given argument.
         *
         * @param from      json object from.
         * @param to        json object to.
         * @param fieldFrom json object field from.
         * @param fieldTo   json object field to.
         */
        void apply(JsonObject from, JsonObject to, String fieldFrom, String fieldTo);

    }

}