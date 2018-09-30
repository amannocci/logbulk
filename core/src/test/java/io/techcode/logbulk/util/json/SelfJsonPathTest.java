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
 * Test for self json path.
 */
public class SelfJsonPathTest {

    @Test(expected = NullPointerException.class)
    public void testGet1() {
        JsonPath path = JsonPath.create("$");
        JsonObject doc = null;
        path.get(doc);
    }

    @Test public void testGet2() {
        JsonPath path = JsonPath.create("$");
        assertEquals(new JsonObject().put("test", "test"), path.get(new JsonObject().put("test", "test")));
    }

    @Test(expected = NullPointerException.class)
    public void testGet3() {
        JsonPath path = JsonPath.create("$");
        JsonArray doc = null;
        path.get(doc);
    }

    @Test public void testGet4() {
        JsonPath path = JsonPath.create("$");
        assertEquals(new JsonArray().add("test"), path.get(new JsonArray().add("test")));
    }

    @Test(expected = NullPointerException.class)
    public void testGet5() {
        JsonPath path = JsonPath.create("$");
        JsonObject doc = null;
        path.get(doc, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testGet6() {
        JsonPath path = JsonPath.create("$");
        path.get(new JsonObject(), null);
    }

    @Test(expected = NullPointerException.class)
    public void testGet7() {
        JsonPath path = JsonPath.create("$");
        JsonArray doc = null;
        path.get(doc, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testGet8() {
        JsonPath path = JsonPath.create("$");
        path.get(new JsonArray(), null);
    }

    @Test public void testGet9() {
        JsonPath path = JsonPath.create("$");
        assertEquals(new JsonObject(), path.get(new JsonObject(), JsonObject.class));
    }

    @Test public void testGet10() {
        JsonPath path = JsonPath.create("$");
        assertEquals(new JsonArray().add("test"), path.get(new JsonArray().add("test"), JsonArray.class));
    }

    @Test public void testGet11() {
        JsonPath path = JsonPath.create("$");
        assertNull(path.get(new JsonObject(), String.class));
    }

    @Test public void testGet12() {
        JsonPath path = JsonPath.create("$");
        assertNull(path.get(new JsonArray().add("test"), String.class));
    }

}
