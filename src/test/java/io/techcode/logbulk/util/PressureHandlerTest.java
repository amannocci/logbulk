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

import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.streams.ReadStream;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Pressure handler test.
 */
public class PressureHandlerTest {

    @Test(expected = NullPointerException.class)
    public void testConstructor1() throws Exception {
        new PressureHandler(null, null);
    }

    @Test public void testConstructor2() {
        // Prepare read stream
        ReadStream mockedStream = mock(ReadStream.class);

        // Test
        new PressureHandler(mockedStream, "test-endpoint");
        verify(mockedStream).endHandler(any());
    }

    @Test public void testHandle1() {
        // Prepare mocks
        ReadStream mockedStream = mock(ReadStream.class);
        Message<String> mockedMessage = mock(Message.class);
        when(mockedMessage.body()).thenReturn("next-endpoint");

        // Test
        PressureHandler handler = new PressureHandler(mockedStream, "test-endpoint");
        handler.handle(mockedMessage);
        verify(mockedStream).pause();
    }

    @Test public void testHandle2() {
        // Prepare mocks
        ReadStream mockedStream = mock(ReadStream.class);
        Message<String> mockedMessage = mock(Message.class);
        when(mockedMessage.body()).thenReturn("next-endpoint");

        // Test
        PressureHandler handler = new PressureHandler(mockedStream, "test-endpoint");
        handler.handle(mockedMessage);
        handler.handle(mockedMessage);
        verify(mockedStream).pause();
        verify(mockedStream).resume();
    }

    @Test public void testHandle3() {
        // Prepare mocks
        EndedReadStream mockedStream = new EndedReadStream();
        Message<String> mockedMessage = mock(Message.class);

        // Test
        PressureHandler handler = new PressureHandler(mockedStream, "test-endpoint");
        mockedStream.fireEnd();
        handler.handle(mockedMessage);
    }

    @Test public void testHandle4() {
        // Prepare mocks
        EndedReadStream mockedStream = new EndedReadStream();

        // Test
        AtomicBoolean called = new AtomicBoolean();
        new PressureHandler(mockedStream, "test-endpoint", event -> called.set(true));
        mockedStream.fireEnd();
        assertTrue(called.get());
    }

    private class EndedReadStream<T> implements ReadStream<T> {

        private Handler<Void> endHandler;

        @Override public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
            throw new RuntimeException();
        }

        @Override public ReadStream<T> handler(Handler<T> handler) {
            throw new RuntimeException();
        }

        @Override public ReadStream<T> pause() {
            throw new RuntimeException();
        }

        @Override public ReadStream<T> resume() {
            throw new RuntimeException();
        }

        @Override public EndedReadStream<T> endHandler(Handler<Void> endHandler) {
            this.endHandler = endHandler;
            return this;
        }

        public void fireEnd() {
            if (endHandler != null) endHandler.handle(null);
        }

    }

}