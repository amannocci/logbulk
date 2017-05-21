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

import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Compiled json path implementation.
 */
public class CompiledJsonPath extends JsonPath {

    // Pattern to validate json path
    private static final Pattern VALID_JSON_PATH = Pattern.compile("\\$((\\.[a-zA-Z]+)|(\\[[0-9]+\\]))+");

    // Pattern to iterate based on json path
    private static final Pattern TREE_JSON_PATH = Pattern.compile("((\\.[a-zA-Z]+)|(\\[[0-9]+\\]))");

    // List of accessors
    private List<Accessor> accessors = Lists.newArrayList();

    /**
     * Create a new json path accessor.
     *
     * @param path json path.
     */
    CompiledJsonPath(String path) {
        super(path);
        checkArgument(VALID_JSON_PATH.matcher(path).matches(), "The path must be a valid jsonpath");
        parse(path.substring(1));

        // Optimize
        if (accessors.size() == 1) {
            accessors = Collections.singletonList(accessors.get(0));
        } else {
            ((ArrayList) accessors).trimToSize();
        }
    }

    @Override public Object get(@NonNull JsonObject doc) {
        return getUnderlying(doc);
    }

    @Override public Object get(@NonNull JsonArray doc) {
        return getUnderlying(doc);
    }

    @Override public <T> T get(@NonNull JsonObject doc, @NonNull Class<T> typed) {
        Object value = getUnderlying(doc);
        return typed.isInstance(value) ? (T) value : null;
    }

    @Override public <T> T get(@NonNull JsonArray doc, @NonNull Class<T> typed) {
        Object value = getUnderlying(doc);
        return typed.isInstance(value) ? (T) value : null;
    }

    /**
     * Get a value based on json path.
     *
     * @param doc json document.
     * @return value if possible, otherwise false.
     */
    private Object getUnderlying(Object doc) {
        // Current document
        Object current = doc;

        // Iterate over each accessor and apply
        Iterator<Accessor> it = accessors.iterator();
        while (it.hasNext() && current != null) {
            current = it.next().get(current);
        }

        // Result
        return current;
    }

    @Override public void put(@NonNull JsonObject doc, Object value) {
        putUnderlying(doc, value);
    }

    @Override public void put(@NonNull JsonArray doc, Object value) {
        putUnderlying(doc, value);
    }

    /**
     * Put a value based on json path.
     *
     * @param doc   json document.
     * @param value value to put.
     */
    private void putUnderlying(Object doc, Object value) {
        // Current document
        Object current = doc;

        // Calculate number of steps
        int step = accessors.size() - 1;

        // We have to prepare tree
        if (step > 0) {
            for (int i = 0; i < step; i++) {
                Accessor currAcc = accessors.get(i);
                if (currAcc.get(current) == null) {
                    Accessor nextAcc = accessors.get(i + 1);
                    if (nextAcc instanceof ObjectAccessor) {
                        Object next = new JsonObject();
                        currAcc.put(current, next);
                        current = next;
                    } else {
                        Object next = new JsonArray();
                        currAcc.put(current, next);
                        current = next;
                    }
                } else {
                    current = currAcc.get(current);
                }
            }
        }

        // Apply last accessor
        accessors.get(step).put(current, value);
    }

    @Override public void remove(@NonNull JsonObject doc) {
        removeUnderlying(doc);
    }

    @Override public void remove(@NonNull JsonArray doc) {
        removeUnderlying(doc);
    }

    /**
     * Remove a value based on json path.
     *
     * @param doc json document.
     */
    private void removeUnderlying(Object doc) {
        // Current document
        Object current = doc;

        // Iterate over each accessor and apply
        Iterator<Accessor> it = accessors.iterator();
        while (current != null) {
            Accessor accessor = it.next();
            if (it.hasNext()) {
                current = accessor.get(current);
            } else {
                accessor.remove(current);
                current = null;
            }
        }
    }

    /**
     * Parse json path.
     *
     * @param path json path.
     */
    private void parse(String path) {
        final Matcher matcher = TREE_JSON_PATH.matcher(path);
        while (matcher.find()) {
            String match = matcher.group(0);
            if (match.startsWith(".")) {
                accessors.add(new ObjectAccessor(match.substring(1)));
            } else {
                accessors.add(new ArrayAccessor(Integer.parseInt(match.substring(1, match.length() - 1))));
            }
        }
    }

    /**
     * Basic accessor.
     */
    @AllArgsConstructor
    private abstract class Accessor<T> {

        private Class<T> typed;

        public Object get(Object doc) {
            if (typed.isInstance(doc)) {
                return getTyped((T) doc);
            } else {
                return null;
            }
        }

        public abstract Object getTyped(T doc);

        public void put(Object doc, Object value) {
            if (typed.isInstance(doc)) {
                putTyped((T) doc, value);
            }
        }

        public abstract void putTyped(T doc, Object value);

        public void remove(Object doc) {
            if (typed.isInstance(doc)) {
                removeTyped((T) doc);
            }
        }

        public abstract void removeTyped(T doc);
    }

    /**
     * Json object accessor.
     */
    private class ObjectAccessor extends Accessor<JsonObject> {
        private String field;

        public ObjectAccessor(String field) {
            super(JsonObject.class);
            this.field = field;
        }

        @Override public Object getTyped(JsonObject doc) {
            return doc.getValue(field);
        }

        @Override public void putTyped(JsonObject doc, Object value) {
            doc.put(field, value);
        }

        @Override public void removeTyped(JsonObject doc) {
            doc.remove(field);
        }
    }

    /**
     * Json array accessor.
     */
    private class ArrayAccessor extends Accessor<JsonArray> {
        private int index;

        public ArrayAccessor(int index) {
            super(JsonArray.class);
            this.index = index;
        }

        @Override public Object getTyped(JsonArray doc) {
            return index < doc.size() ? doc.getValue(index) : null;
        }

        @Override public void putTyped(JsonArray doc, Object value) {
            List list = doc.getList();
            if (index >= list.size()) {
                for (int i = index - list.size(); i > 0; i--) {
                    list.add(null);
                }
                list.add(index, value);
            } else {
                list.set(index, value);
            }
        }

        @Override public void removeTyped(JsonArray doc) {
            doc.remove(index);
        }

    }

}
