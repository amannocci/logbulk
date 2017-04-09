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

import com.google.common.util.concurrent.MoreExecutors;
import io.techcode.logbulk.VertxTestBase;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Test for AsyncInputStream.
 */
@RunWith(VertxUnitRunner.class)
public class AsyncInputStreamTest extends VertxTestBase {

    // Charset UTF-8
    private static final Charset UTF_8 = StandardCharsets.UTF_8;

    // Empty handler
    private static final Handler<Buffer> EMPTY_HANDLER = h -> {
    };

    // Executor service
    private ExecutorService executor;

    @Before public void setUp() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After public void tearDown() {
        if (executor != null) {
            MoreExecutors.shutdownAndAwaitTermination(executor, 1000, TimeUnit.MINUTES);
        }
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor1() throws Exception {
        new AsyncInputStream(null, executor, new ByteArrayInputStream(ArrayUtils.EMPTY_BYTE_ARRAY));
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor2() throws Exception {
        new AsyncInputStream(vertx, null, new ByteArrayInputStream(ArrayUtils.EMPTY_BYTE_ARRAY));
    }

    @Test(expected = NullPointerException.class)
    public void testConstructor3() throws Exception {
        new AsyncInputStream(vertx, executor, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor4() throws Exception {
        new AsyncInputStream(vertx, executor, new ByteArrayInputStream(ArrayUtils.EMPTY_BYTE_ARRAY), -1);
    }

    @Test
    public void testConstructor5() throws Exception {
        new AsyncInputStream(vertx, executor, new ByteArrayInputStream(ArrayUtils.EMPTY_BYTE_ARRAY), 8192);
    }

    @Test
    public void testState1() throws Exception {
        AsyncInputStream stream = createStream();
        assertEquals(1, stream.getStatus());
    }

    @Test public void testState2() {
        AsyncInputStream stream = createStream();
        stream.handler(EMPTY_HANDLER);
        stream.pause();
        assertEquals(0, stream.getStatus());
    }

    @Test public void testState3(TestContext ctx) {
        AsyncInputStream stream = createStream();
        Async async = ctx.async();
        stream.endHandler(h -> {
            ctx.assertEquals(2, stream.getStatus());
            async.complete();
        });
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testEndHandler1(TestContext ctx) {
        AsyncInputStream stream = createStream();
        Async async = ctx.async();
        stream.endHandler(h -> async.complete());
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testEndHandler2(TestContext ctx) {
        AsyncInputStream stream = createStream();
        Async async = ctx.async();
        stream.endHandler(h -> async.complete());
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testIsClosed1(TestContext ctx) {
        AsyncInputStream stream = createStream();
        Async async = ctx.async();
        stream.endHandler(h -> {
            assertTrue(stream.isClosed());
            async.complete();
        });
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testIsClosed2() {
        AsyncInputStream stream = createStream();
        assertFalse(stream.isClosed());
    }

    @Test(expected = NullPointerException.class)
    public void testHandler1() {
        AsyncInputStream stream = createStream();
        stream.handler(null);
    }

    @Test public void testHandler2(TestContext ctx) {
        AsyncInputStream stream = createStream("foobar");
        Async async = ctx.async();
        stream.handler(h -> async.complete());
    }

    @Test public void testExceptionHandler() {
        AsyncInputStream stream = createStream();
        stream.exceptionHandler(th -> {
        });
    }

    @Test public void testTransferredBytes1() {
        AsyncInputStream stream = createStream();
        assertEquals(0, stream.transferredBytes());
    }

    @Test public void testTransferredBytes2(TestContext ctx) {
        AsyncInputStream stream = createStream();
        Async async = ctx.async();
        stream.endHandler(h -> {
            assertEquals(0, stream.transferredBytes());
            async.complete();
        });
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testTransferredBytes3(TestContext ctx) {
        AsyncInputStream stream = createStream("foobar");
        Async async = ctx.async();
        stream.endHandler(h -> {
            assertNotEquals(0, stream.transferredBytes());
            async.complete();
        });
        stream.handler(EMPTY_HANDLER);
    }

    @Test public void testIsEndOfStream1() throws Exception {
        AsyncInputStream stream = createStream();
        assertTrue(stream.isEndOfInput());
    }

    @Test public void testIsEndOfStream2() throws Exception {
        AsyncInputStream stream = createStream("foo");
        assertFalse(stream.isEndOfInput());
    }

    /**
     * Create an async input stream correctly.
     *
     * @return async input stream.
     */
    private AsyncInputStream createStream() {
        return new AsyncInputStream(vertx, executor, new ByteArrayInputStream(ArrayUtils.EMPTY_BYTE_ARRAY), 8192);
    }

    /**
     * Create an async input stream correctly.
     *
     * @param test test.
     * @return async input stream.
     */
    private AsyncInputStream createStream(String test) {
        return new AsyncInputStream(vertx, executor, new ByteArrayInputStream(test.getBytes(UTF_8)), 8192);
    }

}
