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
import lombok.ToString;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Compiled json path implementation.
 */
public class CompiledJsonPath extends JsonPath {

    // Pattern to validate json path
    private static final Pattern VALID_JSON_PATH = Pattern.compile("\\$((\\.[a-zA-Z]+)|(\\[[0-9]+\\]))*");

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
        checkArgument(VALID_JSON_PATH.matcher(path).matches(), "The path must be a valid jsonpath");
        if ("$".equals(path)) {
            accessors.add(new SelfAccessor());
        } else {
            parse(path.substring(1));
        }

        // Optimize
        if (accessors.size() == 1) {
            accessors = Collections.singletonList(accessors.get(0));
        } else {
            ((ArrayList) accessors).trimToSize();
        }
    }

    @Override public <T> T get(@NonNull Object doc) {
        // Current document
        Object current = doc;

        // Iterate over each accessor and apply
        Iterator<Accessor> it = accessors.iterator();
        while (it.hasNext() && current != null) {
            current = it.next().get(current);
        }

        // Result
        return (T) current;
    }

    @Override public void put(@NonNull Object doc, @NonNull Object value) {
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
                    } else if (nextAcc instanceof ArrayAccessor) {
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

    @Override public void remove(@NonNull Object doc) {
        // Current document
        Object current = doc;

        // Iterate over each accessor and apply
        ListIterator<Accessor> it = accessors.listIterator();
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
            } else if (match.startsWith("[") && match.endsWith("]")) {
                accessors.add(new ArrayAccessor(Integer.parseInt(match.substring(1, match.length() - 1))));
            }
        }
    }

    /**
     * Basic accessor.
     */
    private interface Accessor {
        Object get(Object doc);

        default void put(Object doc, Object value) {
        }

        default void remove(Object doc) {
        }
    }

    /**
     * Self accessor.
     */
    private class SelfAccessor implements Accessor {
        @Override public Object get(Object doc) {
            return doc;
        }
    }

    /**
     * Json object accessor.
     */
    @AllArgsConstructor
    @ToString
    private class ObjectAccessor implements Accessor {
        private String field;

        @Override public Object get(Object doc) {
            if (doc instanceof JsonObject) {
                return ((JsonObject) doc).getValue(field);
            } else {
                return null;
            }
        }

        @Override public void put(Object doc, Object value) {
            if (doc instanceof JsonObject) {
                ((JsonObject) doc).put(field, value);
            }
        }

        @Override public void remove(Object doc) {
            if (doc instanceof JsonObject) {
                ((JsonObject) doc).remove(field);
            }
        }
    }

    /**
     * Json array accessor.
     */
    @AllArgsConstructor
    @ToString
    private class ArrayAccessor implements Accessor {
        private int index;

        @Override public Object get(Object doc) {
            if (doc instanceof JsonArray) {
                JsonArray array = (JsonArray) doc;
                return index < array.size() ? array.getValue(index) : null;
            } else {
                return null;
            }
        }

        @Override public void put(Object doc, Object value) {
            if (doc instanceof JsonArray) {
                List list = ((JsonArray) doc).getList();
                if (index >= list.size()) {
                    for (int i = index - list.size(); i > 0; i--) {
                        list.add(null);
                    }
                    list.add(index, value);
                } else {
                    list.set(index, value);
                }
            }
        }

        @Override public void remove(Object doc) {
            if (doc instanceof JsonArray) {
                ((JsonArray) doc).remove(index);
            }
        }

    }

}
