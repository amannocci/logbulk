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
package io.techcode.logbulk.io;

import com.google.common.collect.Maps;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test for Configuration.
 */
public class ConfigurationTest {

    @Test public void testConstructor1() {
        Configuration configuration = new Configuration();
        assertTrue(configuration.isEmpty());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor2() {
        JsonObject test = null;
        new Configuration(test);
    }

    @Test public void testConstructor3() {
        Configuration configuration = new Configuration("{}");
        assertTrue(configuration.isEmpty());
    }

    @Test public void testConstructor4() {
        Configuration configuration = new Configuration(Maps.newHashMap());
        assertTrue(configuration.isEmpty());
    }

    @Test(expected = DecodeException.class)
    public void testConstructor5() {
        String test = null;
        new Configuration(test);
    }

    @Test public void testConstructor6() {
        Map<String, Object> test = null;
        Configuration configuration = new Configuration(test);
        assertTrue(configuration.isEmpty());
    }

    @Test public void testGetInteger1() {
        Configuration conf = new Configuration();
        conf.put("test", 1);
        assertEquals(1, (int) conf.getInteger("test"));
    }

    @Test public void testGetInteger2() {
        Configuration conf = new Configuration();
        conf.put("test", "1");
        assertEquals(1, (int) conf.getInteger("test"));
    }

    @Test public void testGetInteger3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getInteger("test"));
    }

    @Test public void testGetInteger4() {
        assertEquals(1, (int) new Configuration().getInteger("test", 1));
    }

    @Test(expected = ClassCastException.class)
    public void testGetInteger5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getInteger("test");
    }

    @Test public void testGetLong1() {
        Configuration conf = new Configuration();
        conf.put("test", 1L);
        assertEquals(1L, (long) conf.getLong("test"));
    }

    @Test public void testGetLong2() {
        Configuration conf = new Configuration();
        conf.put("test", "1");
        assertEquals(1, (long) conf.getLong("test"));
    }

    @Test public void testGetLong3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getLong("test"));
    }

    @Test public void testGetLong4() {
        assertEquals(1L, (long) new Configuration().getLong("test", 1L));
    }

    @Test(expected = ClassCastException.class)
    public void testGetLong5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getLong("test");
    }

    @Test public void testGetDouble1() {
        Configuration conf = new Configuration();
        conf.put("test", 0.1D);
        assertEquals(0.1D, conf.getDouble("test"), 0.001);
    }

    @Test public void testGetDouble2() {
        Configuration conf = new Configuration();
        conf.put("test", "0.1");
        assertEquals(0.1D, conf.getDouble("test"), 0.001);
    }

    @Test public void testGetDouble3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getDouble("test"));
    }

    @Test public void testGetDouble4() {
        assertEquals(0.1D, new Configuration().getDouble("test", 0.1D), 0.001);
    }

    @Test(expected = ClassCastException.class)
    public void testGetDouble5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getDouble("test");
    }

    @Test public void testGetFloat1() {
        Configuration conf = new Configuration();
        conf.put("test", 0.1F);
        assertEquals(0.1F, conf.getFloat("test"), 0.001);
    }

    @Test public void testGetFloat2() {
        Configuration conf = new Configuration();
        conf.put("test", "0.1");
        assertEquals(0.1F, conf.getFloat("test"), 0.001);
    }

    @Test public void testGetFloat3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getFloat("test"));
    }

    @Test public void testGetFloat4() {
        assertEquals(0.1F, new Configuration().getFloat("test", 0.1F), 0.001);
    }

    @Test(expected = ClassCastException.class)
    public void testGetFloat5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getFloat("test");
    }

    @Test public void testGetBoolean1() {
        Configuration conf = new Configuration();
        conf.put("test", true);
        assertTrue(conf.getBoolean("test"));
    }

    @Test public void testGetBoolean2() {
        Configuration conf = new Configuration();
        conf.put("test", "true");
        assertTrue(conf.getBoolean("test"));
    }

    @Test public void testGetBoolean3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getBoolean("test"));
    }

    @Test public void testGetBoolean4() {
        assertTrue(new Configuration().getBoolean("test", true));
    }

    @Test public void testGetBoolean5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        assertFalse(conf.getBoolean("test"));
    }

    @Test public void testGetString1() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        assertEquals("test", conf.getString("test"));
    }

    @Test public void testGetString2() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getString("test"));
    }

    @Test public void testGetString3() {
        assertEquals("test", new Configuration().getString("test", "test"));
    }

    @Test public void testGetJsonObject1() {
        Configuration conf = new Configuration();
        conf.put("test", new JsonObject());
        assertEquals(new JsonObject(), conf.getJsonObject("test"));
    }

    @Test public void testGetJsonObject2() {
        Configuration conf = new Configuration();
        conf.put("test", "{}");
        assertEquals(new JsonObject(), conf.getJsonObject("test"));
    }

    @Test public void testGetJsonObject3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getJsonObject("test"));
    }

    @Test public void testGetJsonObject4() {
        assertEquals(new JsonObject(), new Configuration().getJsonObject("test", new JsonObject()));
    }

    @Test(expected = DecodeException.class)
    public void testGetJsonObject5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getJsonObject("test");
    }

    @Test public void testGetJsonArray1() {
        Configuration conf = new Configuration();
        conf.put("test", new JsonArray());
        assertEquals(new JsonArray(), conf.getJsonArray("test"));
    }

    @Test public void testGetJsonArray2() {
        Configuration conf = new Configuration();
        conf.put("test", "[]");
        assertEquals(new JsonArray(), conf.getJsonArray("test"));
    }

    @Test public void testGetJsonArray3() {
        Configuration conf = new Configuration();
        conf.putNull("test");
        assertNull(conf.getJsonArray("test"));
    }

    @Test public void testGetJsonArray4() {
        assertEquals(new JsonArray(), new Configuration().getJsonArray("test", new JsonArray()));
    }

    @Test(expected = DecodeException.class)
    public void testGetJsonArray5() {
        Configuration conf = new Configuration();
        conf.put("test", "test");
        conf.getJsonArray("test");
    }

}
