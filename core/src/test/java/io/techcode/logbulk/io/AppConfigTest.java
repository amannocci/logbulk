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
package io.techcode.logbulk.io;

import com.typesafe.config.ConfigValue;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for AppConfig.
 */
@RunWith(JUnitParamsRunner.class)
public class AppConfigTest {

    @Test public void testConstructor() {
        new AppConfig();
    }

    @Parameters({
            "input",
            "transform",
            "output"
    })
    @Test public void testComponents(String component) {
        Set<Map.Entry<String, ConfigValue>> config = new AppConfig().components();
        assertTrue(config.stream().map(Map.Entry::getKey).anyMatch(x -> x.equals(component)));
    }

    @Test public void testInputs() {
        Set<Map.Entry<String, ConfigValue>> config = new AppConfig().inputs();
        assertTrue(config.isEmpty());
    }

    @Test public void testOutputs() {
        Set<Map.Entry<String, ConfigValue>> config = new AppConfig().outputs();
        assertTrue(config.isEmpty());
    }

    @Test public void testTransforms() {
        Set<Map.Entry<String, ConfigValue>> config = new AppConfig().transforms();
        assertTrue(config.isEmpty());
    }

    @Test public void testRoutes() {
        JsonObject config = new AppConfig().routes();
        assertTrue(config.isEmpty());
    }

    @Test public void testSettings() {
        JsonObject config = new AppConfig().settings();
        assertTrue(config.isEmpty());
    }

    @Test public void testToString() {
        String config = new AppConfig().toString();
        assertTrue(config.contains("setting{}"));
        assertTrue(config.contains("input{}"));
        assertTrue(config.contains("output{}"));
        assertTrue(config.contains("transform{}"));
        assertTrue(config.contains("component{input{},output{},transform{test=\"io.techcode.logbulk.Logbulk\"}}"));
    }

}