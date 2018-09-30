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
import org.junit.Before;
import org.junit.Test;

import static io.techcode.logbulk.util.Binder.*;
import static org.junit.Assert.*;

/**
 * Test for Binder.
 */
public class BinderTest {

    // From object
    private JsonObject from;

    // To object
    private JsonObject to;

    // Binder
    private Binder binder;

    @Before public void setUp() {
        from = new JsonObject();
        to = new JsonObject();
        binder = new Binder();
    }

    @Test(expected = NullPointerException.class)
    public void testTo() throws Exception {
        binder.to(null);
    }

    @Test(expected = NullPointerException.class)
    public void testFrom() throws Exception {
        binder.from(null);
    }

    @Test public void testIdentityFunction1() throws Exception {
        // Prepare for test
        Boolean excepted = true;
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getValue("obj"));
    }

    @Test public void testIdentityFunction2() throws Exception {
        // Prepare for test
        Integer excepted = 1;
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getInteger("obj"));
    }

    @Test public void testIdentityFunction3() throws Exception {
        // Prepare for test
        Long excepted = 1L;
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getLong("obj"));
    }

    @Test public void testIdentityFunction4() throws Exception {
        // Prepare for test
        Float excepted = 0.0F;
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getFloat("obj"));
    }

    @Test public void testIdentityFunction5() throws Exception {
        // Prepare for test
        Double excepted = 0.0D;
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getDouble("obj"));
    }

    @Test public void testIdentityFunction6() throws Exception {
        // Prepare for test
        String excepted = "foobar";
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertEquals(excepted, to.getString("obj"));
    }

    @Test public void testIdentityFunction7() throws Exception {
        // Test
        binder.from(from).to(to).bindOrNull("object", "obj", IDENTITY);
        assertNull(to.getString("obj"));
    }

    @Test public void testIdentityFunction8() throws Exception {
        // Prepare for test
        String excepted = "foobar";
        from.put("object", excepted);

        // Test
        binder.from(from).to(to).bindOrNull("object", "obj", IDENTITY);
        assertEquals(excepted, to.getString("obj"));
    }

    @Test public void testIdentityFunction9() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", IDENTITY);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testAnyToStringFunction1() throws Exception {
        // Prepare for test
        from.put("object", true);

        // Test
        binder.from(from).to(to).bind("object", "obj", ANY_TO_STRING);
        assertEquals("true", to.getString("obj"));
    }

    @Test public void testAnyToStringFunction2() throws Exception {
        // Prepare for test
        from.put("object", 1);

        // Test
        binder.from(from).to(to).bind("object", "obj", ANY_TO_STRING);
        assertEquals("1", to.getString("obj"));
    }

    @Test public void testAnyToStringFunction3() throws Exception {
        // Prepare for test
        from.put("object", 0.0);

        // Test
        binder.from(from).to(to).bind("object", "obj", ANY_TO_STRING);
        assertEquals("0.0", to.getString("obj"));
    }

    @Test public void testAnyToStringFunction4() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", ANY_TO_STRING);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testStringToIntFunction1() throws Exception {
        // Prepare for test
        from.put("object", "1");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_INT);
        assertTrue(1 == to.getInteger("obj"));
    }

    @Test public void testStringToIntFunction2() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_INT);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testStringToLongFunction1() throws Exception {
        // Prepare for test
        from.put("object", "1");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_LONG);
        assertTrue(1 == to.getLong("obj"));
    }

    @Test public void testStringToLongFunction2() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_LONG);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testStringToFloatFunction1() throws Exception {
        // Prepare for test
        from.put("object", "0.0");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_FLOAT);
        assertEquals(0.0, to.getFloat("obj"), 0.001F);
    }

    @Test public void testStringToFloatFunction2() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_FLOAT);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testStringToDoubleFunction1() throws Exception {
        // Prepare for test
        from.put("object", "0.0");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_DOUBLE);
        assertEquals(0.0, to.getDouble("obj"), 0.001F);
    }

    @Test public void testStringToDoubleFunction2() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_DOUBLE);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testStringToBooleanFunction1() throws Exception {
        // Prepare for test
        from.put("object", "true");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_BOOLEAN);
        assertTrue(to.getBoolean("obj"));
    }

    @Test public void testStringToBooleanFunction2() throws Exception {
        // Prepare for test
        from.put("object", "false");

        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_BOOLEAN);
        assertFalse(to.getBoolean("obj"));
    }

    @Test public void testStringToBooleanFunction3() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", STRING_TO_BOOLEAN);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testIntToBooleanFunction1() throws Exception {
        // Prepare for test
        from.put("object", 0);

        // Test
        binder.from(from).to(to).bind("object", "obj", INT_TO_BOOLEAN);
        assertFalse(to.getBoolean("obj"));
    }

    @Test public void testIntToBooleanFunction2() throws Exception {
        // Prepare for test
        from.put("object", 1);

        // Test
        binder.from(from).to(to).bind("object", "obj", INT_TO_BOOLEAN);
        assertTrue(to.getBoolean("obj"));
    }

    @Test public void testIntToBooleanFunction3() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", INT_TO_BOOLEAN);
        assertFalse(to.containsKey("obj"));
    }

    @Test public void testLongToBooleanFunction1() throws Exception {
        // Prepare for test
        from.put("object", 0L);

        // Test
        binder.from(from).to(to).bind("object", "obj", LONG_TO_BOOLEAN);
        assertFalse(to.getBoolean("obj"));
    }

    @Test public void testLongToBooleanFunction2() throws Exception {
        // Prepare for test
        from.put("object", 1L);

        // Test
        binder.from(from).to(to).bind("object", "obj", LONG_TO_BOOLEAN);
        assertTrue(to.getBoolean("obj"));
    }

    @Test public void testLongToBooleanFunction3() throws Exception {
        // Test
        binder.from(from).to(to).bind("object", "obj", LONG_TO_BOOLEAN);
        assertFalse(to.containsKey("obj"));
    }

}