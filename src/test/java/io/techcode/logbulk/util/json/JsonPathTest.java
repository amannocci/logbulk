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
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Test for json path.
 */
public class JsonPathTest {

    @Test public void testCreate1() {
        assertTrue(JsonPath.create("test") instanceof DirectJsonPath);
    }

    @Test public void testCreate2() {
        assertTrue(JsonPath.create("$.test") instanceof DirectJsonPath);
    }

    @Test public void testCreate3() {
        assertTrue(JsonPath.create("$.test.test") instanceof CompiledJsonPath);
    }

    @Test public void testCreate4() {
        assertTrue(JsonPath.create("$.test[0]") instanceof CompiledJsonPath);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreate5() {
        JsonPath.create(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreate6() {
        JsonPath.create("");
    }

    @Test public void testGet1() {
        JsonObject mock = mock(JsonObject.class);
        assertNull(new Impl("test").get(mock));
        verifyZeroInteractions(mock);
    }

    @Test(expected = NullPointerException.class)
    public void testGet2() {
        JsonObject value = null;
        new Impl("test").get(value);
    }

    @Test public void testGet3() {
        JsonArray mock = mock(JsonArray.class);
        assertNull(new Impl("test").get(mock));
        verifyZeroInteractions(mock);
    }

    @Test(expected = NullPointerException.class)
    public void testGet4() {
        JsonArray value = null;
        new Impl("test").get(value);
    }

    @Test public void testGet5() {
        JsonArray mock = mock(JsonArray.class);
        assertNull(new Impl("test").get(mock, String.class));
        verifyZeroInteractions(mock);
    }

    @Test public void testGet6() {
        JsonObject mock = mock(JsonObject.class);
        assertNull(new Impl("test").get(mock, String.class));
        verifyZeroInteractions(mock);
    }

    @Test(expected = NullPointerException.class)
    public void testGet7() {
        JsonArray value = null;
        new Impl("test").get(value, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testGet8() {
        JsonObject value = null;
        new Impl("test").get(value, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void testPut1() {
        JsonObject value = null;
        new Impl("test").put(value, "test");
    }

    @Test(expected = NullPointerException.class)
    public void testPut2() {
        JsonArray value = null;
        new Impl("test").put(value, "test");
    }

    @Test public void testPut3() {
        JsonArray mock = mock(JsonArray.class);
        new Impl("test").put(mock, "test");
        verifyZeroInteractions(mock);
    }

    @Test public void testPut4() {
        JsonObject mock = mock(JsonObject.class);
        new Impl("test").put(mock, "test");
        verifyZeroInteractions(mock);
    }

    @Test(expected = NullPointerException.class)
    public void testRemove1() {
        JsonObject value = null;
        new Impl("test").remove(value);
    }

    @Test(expected = NullPointerException.class)
    public void testRemove2() {
        JsonArray value = null;
        new Impl("test").remove(value);
    }

    @Test public void testRemove3() {
        JsonArray mock = mock(JsonArray.class);
        new Impl("test").remove(mock);
        verifyZeroInteractions(mock);
    }

    @Test public void testRemove4() {
        JsonObject mock = mock(JsonObject.class);
        new Impl("test").remove(mock);
        verifyZeroInteractions(mock);
    }

    @Test public void testToString() {
        assertEquals("test", new Impl("test").toString());
    }

    @Test public void testCompareTo1() {
        assertEquals(-1, new Impl("a").compareTo(new Impl("b")));
    }

    @Test public void testCompareTo2() {
        assertEquals(1, new Impl("b").compareTo(new Impl("a")));
    }

    @Test public void testCompareTo3() {
        assertEquals(0, new Impl("A").compareTo(new Impl("a")));
    }

    private class Impl extends JsonPath {
        /**
         * Create a new json path.
         *
         * @param path json path.
         */
        protected Impl(String path) {
            super(path);
        }
    }

}
