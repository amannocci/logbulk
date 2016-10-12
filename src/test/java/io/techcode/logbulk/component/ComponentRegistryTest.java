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
import io.techcode.logbulk.io.AppConfig;
import io.vertx.core.Verticle;
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
        Verticle mockedVerticle = mock(Verticle.class);
        AppConfig mockedConfig = mock(AppConfig.class);
        when(mockedConfig.components()).thenReturn(Sets.newHashSet());

        // Test
        ComponentRegistry registry = new ComponentRegistry(mockedVerticle);
        registry.registerAll(mockedConfig);
        verify(mockedConfig).components();
    }

    private void testGetComponent(String component) {
        // Prepare mocks
        Verticle mockedVerticle = mock(Verticle.class);

        // Test
        ComponentRegistry registry = new ComponentRegistry(mockedVerticle);
        registry.registerAll(new AppConfig());
        assertNotNull(registry.getComponent(component));
    }

    @Parameters({
            "input.stdin",
            "input.file",
            "input.tcp",
            "input.exec",
            "input.mysql",
            "input.heartbeat",
            "transform.dispatch",
            "transform.grok",
            "transform.date",
            "transform.json",
            "transform.mutate",
            "transform.metric",
            "transform.limiter",
            "transform.anonymise",
            "transform.csv",
            "output.file",
            "output.stdout",
            "output.elasticsearch",
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