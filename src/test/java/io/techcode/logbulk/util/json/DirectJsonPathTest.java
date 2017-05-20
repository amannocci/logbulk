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
package io.techcode.logbulk.util.json;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Test for direct json path.
 */
public class DirectJsonPathTest {

    @Test public void testGet1() {
        JsonPath path = JsonPath.create("$.test");
        assertEquals("test", path.get(new JsonObject().put("test", "test")));
    }

    @Test public void testGet2() {
        JsonPath path = JsonPath.create("$");
        assertEquals(new JsonObject(), path.get(new JsonObject()));
    }

    @Test public void testGet3() {
        JsonPath path = JsonPath.create("$[0]");
        assertEquals(new JsonObject(), path.get(new JsonArray().add(new JsonObject())));
    }

    @Test public void testGet4() {
        JsonPath path = JsonPath.create("$[1]");
        assertNull(path.get(new JsonArray().add(new JsonObject())));
    }

    @Test public void testGet5() {
        JsonPath path = JsonPath.create("$.test");
        assertEquals(new JsonArray(), path.get(new JsonObject().put("test", new JsonArray())));
    }

    @Test public void testGet6() {
        JsonPath path = JsonPath.create("$.test[0].name");
        assertEquals("test", path.get(new JsonObject().put("test", new JsonArray().add(new JsonObject().put("name", "test")))));
    }

    @Test public void testPut1() {
        JsonPath path = JsonPath.create("$.test");
        JsonObject doc = new JsonObject();
        path.put(doc, "name");
        assertEquals("name", doc.getString("test"));
    }

    @Test public void testPut2() {
        JsonPath path = JsonPath.create("$[1]");
        JsonArray doc = new JsonArray();
        path.put(doc, "name");
        assertNull(doc.getString(0));
        assertEquals("name", doc.getString(1));
    }

    @Test public void testPut3() {
        JsonPath path = JsonPath.create("$.test[1]");
        JsonObject doc = new JsonObject();
        path.put(doc, "name");
        assertEquals(new JsonObject().put("test", new JsonArray().addNull().add("name")), doc);
    }

    @Test public void testRemove1() {
        JsonPath path = JsonPath.create("$.test");
        JsonObject doc = new JsonObject().put("test", new JsonObject().put("name", "foobar"));
        path.remove(doc);
        assertEquals(new JsonObject(), doc);
    }

    @Test public void testRemove2() {
        JsonPath path = JsonPath.create("$.test.name");
        JsonObject doc = new JsonObject().put("test", new JsonObject().put("name", "foobar"));
        path.remove(doc);
        assertEquals(new JsonObject().put("test", new JsonObject()), doc);
    }

}
