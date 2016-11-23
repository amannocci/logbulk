/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2016
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

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Binder helper for Json Object binding.
 */
public class Binder {

    // Some functions
    public static final Function IDENTITY = (from, to, fieldFrom, fieldTo) -> {
        Object value = from.getValue(fieldFrom);
        if (value != null) to.put(fieldTo, value);
    };
    public static final Function ANY_TO_STRING = (from, to, fieldFrom, fieldTo) -> {
        Object value = from.getValue(fieldFrom);
        if (value != null) to.put(fieldTo, String.valueOf(value));
    };
    public static final Function STRING_TO_INT = (from, to, fieldFrom, fieldTo) -> {
        String value = from.getString(fieldFrom);
        if (value != null) to.put(fieldTo, Integer.parseInt(value));
    };
    public static final Function STRING_TO_LONG = (from, to, fieldFrom, fieldTo) -> {
        String value = from.getString(fieldFrom);
        if (value != null) to.put(fieldTo, Long.parseLong(value));
    };
    public static final Function STRING_TO_FLOAT = (from, to, fieldFrom, fieldTo) -> {
        String value = from.getString(fieldFrom);
        if (value != null) to.put(fieldTo, Float.parseFloat(value));
    };
    public static final Function STRING_TO_DOUBLE = (from, to, fieldFrom, fieldTo) -> {
        String value = from.getString(fieldFrom);
        if (value != null) to.put(fieldTo, Double.parseDouble(value));
    };
    public static final Function STRING_TO_BOOLEAN = (from, to, fieldFrom, fieldTo) -> {
        String value = from.getString(fieldFrom);
        if (value != null) to.put(fieldTo, Boolean.parseBoolean(value));
    };
    public static final Function INT_TO_BOOLEAN = (from, to, fieldFrom, fieldTo) -> {
        Integer value = from.getInteger(fieldFrom);
        if (value != null) to.put(fieldTo, value != 0);
    };
    public static final Function LONG_TO_BOOLEAN = (from, to, fieldFrom, fieldTo) -> {
        Long value = from.getLong(fieldFrom);
        if (value != null) to.put(fieldTo, value != 0);
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
    public Binder from(JsonObject from) {
        this.from = checkNotNull(from, "The json object from can't be null");
        return this;
    }

    /**
     * Returns same object for chaining.
     *
     * @param to to object.
     * @return same object for chaining.
     */
    public Binder to(JsonObject to) {
        this.to = checkNotNull(to, "The json object to can't be null");
        return this;
    }

    /**
     * Bind a field of from object to
     *
     * @param from     from field to bind.
     * @param to       to field to bind.
     * @param function transformation to apply
     * @return same object for chaining.
     */
    public Binder bind(String from, String to, Function function) {
        function.apply(this.from, this.to, from, to);
        return this;
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