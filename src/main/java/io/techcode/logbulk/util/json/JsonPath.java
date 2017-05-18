/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2017
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
package io.techcode.logbulk.util.json;

import com.google.common.base.Strings;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Json path implementation.
 */
@EqualsAndHashCode(of = {"path"})
public abstract class JsonPath implements Comparable<JsonPath> {

    // Path
    protected String path;

    /**
     * Create a new json path.
     *
     * @param path json path.
     */
    protected JsonPath(@NonNull String path) {
        this.path = path;
    }

    /**
     * Create a new json path.
     *
     * @param path json path.
     * @return new json path.
     */
    public static JsonPath create(String path) {
        checkArgument(!Strings.isNullOrEmpty(path), "The json path must be valid");
        if (path.startsWith("$")) {
            return new CompiledJsonPath(path);
        } else {
            return new DirectJsonPath(path);
        }
    }

    /**
     * Get a value based on json path.
     *
     * @param doc json document.
     * @return value if possible, otherwise false.
     */
    public Object get(@NonNull JsonObject doc) {
        return null;
    }

    /**
     * Get a value based on json path.
     *
     * @param doc json document.
     * @return value if possible, otherwise false.
     */
    public Object get(@NonNull JsonArray doc) {
        return null;
    }

    /**
     * Get a value based on json path.
     *
     * @param doc json document.
     * @param <T> type of value.
     * @return value if possible, otherwise false.
     */
    public <T> T get(@NonNull JsonObject doc, Class<T> typed) {
        return null;
    }

    /**
     * Get a value based on json path.
     *
     * @param doc json document.
     * @param <T> type of value.
     * @return value if possible, otherwise false.
     */
    public <T> T get(@NonNull JsonArray doc, Class<T> typed) {
        return null;
    }

    /**
     * Put a value based on json path.
     *
     * @param doc   json document.
     * @param value value to put.
     */
    public void put(@NonNull JsonObject doc, @NonNull Object value) {
    }

    /**
     * Put a value based on json path.
     *
     * @param doc   json document.
     * @param value value to put.
     */
    public void put(@NonNull JsonArray doc, @NonNull Object value) {
    }

    /**
     * Remove a value based on json path.
     *
     * @param doc json document.
     */
    public void remove(@NonNull JsonObject doc) {
    }

    /**
     * Remove a value based on json path.
     *
     * @param doc json document.
     */
    public void remove(@NonNull JsonArray doc) {
    }

    @Override public String toString() {
        return path;
    }

    @Override public int compareTo(JsonPath o) {
        return String.CASE_INSENSITIVE_ORDER.compare(path, o.path);
    }

}