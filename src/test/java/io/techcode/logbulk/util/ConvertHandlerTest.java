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

import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.logging.MessageException;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for ConvertHandler.
 */
public class ConvertHandlerTest {

    private static final Packet VALID_PACKET = Packet.builder()
            .header(Packet.Header.builder()
                    .source("foobar")
                    .route("foobar")
                    .build())
            .body(new JsonObject().put("foobar", "test"))
            .build();

    private static final Packet INVALID_PACKET = Packet.builder()
            .header(Packet.Header.builder()
                    .source("foobar")
                    .route("foobar")
                    .build())
            .body(new JsonObject())
            .build();

    @Test public void testHandle1() throws Exception {
        Message<Packet> mockedMessage = mock(Message.class);
        when(mockedMessage.body()).thenReturn(VALID_PACKET);
        Impl impl = new Impl();
        impl.handle(mockedMessage);
        assertFalse(impl.fallbackCalled);
    }

    @Test public void testHandle2() throws Exception {
        Message<Packet> mockedMessage = mock(Message.class);
        when(mockedMessage.body()).thenReturn(INVALID_PACKET);
        Impl impl = new Impl();
        impl.handle(mockedMessage);
        assertTrue(impl.fallbackCalled);
    }

    @Test public void testHandleFallback() throws Exception {
        ConvertHandler handler = packet -> {
        };
        handler.handleFallback(VALID_PACKET, new MessageException("foobar"));
    }

    private class Impl implements ConvertHandler {
        private boolean fallbackCalled = false;

        @Override public void handle(Packet packet) {
            if (packet.getBody().isEmpty()) {
                throw new NullPointerException();
            }
        }

        @Override public void handleFallback(Packet packet, Throwable th) {
            fallbackCalled = true;
        }
    }

}