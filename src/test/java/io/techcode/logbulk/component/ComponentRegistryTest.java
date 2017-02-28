/*
 * The MIT License (MIT)
 * <p/>
 * Copyright (c) 2016
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
package io.techcode.logbulk.component;

import com.google.common.collect.Sets;
import io.techcode.logbulk.Logbulk;
import io.techcode.logbulk.io.AppConfig;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

/**
 * Component registry test
 */
@RunWith(JUnitParamsRunner.class)
public class ComponentRegistryTest {

    @Test(expected = NullPointerException.class)
    public void testConstructor1() throws Exception {
        new ComponentRegistry(null);
    }

    @Test public void testRegisterAll() {
        // Prepare mocks
        Logbulk mockedVerticle = mock(Logbulk.class);
        AppConfig mockedConfig = mock(AppConfig.class);
        when(mockedVerticle.getConfig()).thenReturn(mockedConfig);
        when(mockedConfig.routes()).thenReturn(new JsonObject());
        when(mockedConfig.components()).thenReturn(Sets.newHashSet());

        // Test
        new ComponentRegistry(mockedVerticle);
        verify(mockedConfig).components();
    }

    @Test public void testAnalyzeRoutes() {
        // Prepare mocks
        Logbulk mockedVerticle = mock(Logbulk.class);
        AppConfig mockedConfig = mock(AppConfig.class);
        when(mockedVerticle.getConfig()).thenReturn(mockedConfig);
        when(mockedConfig.routes()).thenReturn(new JsonObject());
        when(mockedConfig.components()).thenReturn(Sets.newHashSet());

        // Test
        new ComponentRegistry(mockedVerticle);
        verify(mockedConfig).routes();
    }

    private void testGetComponent(String component) {
        // Prepare mocks
        Logbulk mockedVerticle = mock(Logbulk.class);
        when(mockedVerticle.getConfig()).thenReturn(new AppConfig());

        // Test
        ComponentRegistry registry = new ComponentRegistry(mockedVerticle);
        registry.registerAll();
        assertNotNull(registry.getComponent(component));
    }

    @Parameters({
            "input.tcp",
            "input.mysql",
            "input.heartbeat",
            "input.rabbitmq",
            "transform.grok",
            "transform.json",
            "transform.mutate",
            "transform.metric",
            "transform.limiter",
            "transform.regex",
            "output.rabbitmq",
            "output.mongo"
    })
    @Test public void testGetComponent1(String component) {
        testGetComponent(component);
    }

    @Parameters({"not.exist"})
    @Test(expected = IllegalStateException.class)
    public void testGetComponent2(String component) {
        testGetComponent(component);
    }

}