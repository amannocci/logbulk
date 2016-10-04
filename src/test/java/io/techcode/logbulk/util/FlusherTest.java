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
package io.techcode.logbulk.util;

import io.techcode.logbulk.VertxTestBase;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

/**
 * Test for Flusher.
 */
@RunWith(VertxUnitRunner.class)
public class FlusherTest extends VertxTestBase {

    @Test(expected = NullPointerException.class)
    public void testConstructor1() throws Exception {
        new Flusher(null, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor2() throws Exception {
        new Flusher(vertx, -1);
    }

    @Test public void testFlush1() throws Exception {
        Flusher flusher = new Flusher(vertx, 10);
        final boolean[] called = {false};
        flusher.handler(h -> called[0] = true);
        flusher.flush();
        assertTrue(called[0]);
    }

    @Test public void testFlush2(TestContext ctx) throws Exception {
        Flusher flusher = new Flusher(vertx, 100);
        final boolean[] called = {false};
        flusher.handler(h -> called[0] = true);
        flusher.start();
        vertx.setTimer(150, h -> ctx.assertTrue(called[0]));
    }

    @Test public void testFlush3(TestContext ctx) throws Exception {
        Flusher flusher = new Flusher(vertx, 100);
        final boolean[] called = {false};
        flusher.handler(h -> called[0] = true);
        flusher.start();
        vertx.setTimer(150, h -> flusher.flushed());
        vertx.setTimer(200, h -> ctx.assertFalse(called[0]));
    }

    @Test public void testFlush4(TestContext ctx) throws Exception {
        Flusher flusher = new Flusher(vertx, 100);
        final boolean[] called = {false};
        flusher.handler(h -> called[0] = true);
        flusher.start();
        flusher.stop();
        vertx.setTimer(200, h -> ctx.assertFalse(called[0]));
    }

}
