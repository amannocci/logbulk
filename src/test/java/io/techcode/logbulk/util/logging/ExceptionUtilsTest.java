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
package io.techcode.logbulk.util.logging;

import io.vertx.core.json.JsonArray;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

import static org.junit.Assert.*;

/**
 * Test for ExceptionUtils.
 */
public class ExceptionUtilsTest {

    @Test public void testConstructor() throws Exception {
        Constructor<ExceptionUtils> constructor = ExceptionUtils.class.getDeclaredConstructor();
        assertTrue(Modifier.isPrivate(constructor.getModifiers()));
        constructor.setAccessible(true);
        constructor.newInstance();
    }

    @Test public void testGetStacktrace1() {
        JsonArray result = ExceptionUtils.getStackTrace(new NullPointerException());
        assertNotEquals(0, result);
    }

    @Test public void testGetStacktrace2() {
        JsonArray base = new JsonArray().add("foobar");
        JsonArray result = ExceptionUtils.getStackTrace(base, new NullPointerException());
        assertEquals(base.getString(0), result.getString(0));
    }

    @Test public void testGetStacktrace3() {
        JsonArray result = ExceptionUtils.getStackTrace(new MessageException("foobar"));
        assertEquals(1, result.size());
        assertEquals("foobar", result.getString(0));
    }


}