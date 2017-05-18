/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2017
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
package io.techcode.logbulk.util.json;

import io.vertx.core.json.JsonObject;
import lombok.NonNull;

/**
 * Direct json path implementation.
 */
public class DirectJsonPath extends JsonPath {

    /**
     * Create a new direct json path.
     *
     * @param field target field.
     */
    DirectJsonPath(String field) {
        super(field);
    }

    @Override public Object get(@NonNull JsonObject doc) {
        return doc.getValue(path);
    }

    @Override public <T> T get(@NonNull JsonObject doc, @NonNull Class<T> typed) {
        Object value = doc.getValue(path);
        return typed.isInstance(value) ? (T) value : null;
    }

    @Override public void put(@NonNull JsonObject doc, Object value) {
        doc.put(path, value);
    }

    @Override public void remove(@NonNull JsonObject doc) {
        doc.remove(path);
    }

}
