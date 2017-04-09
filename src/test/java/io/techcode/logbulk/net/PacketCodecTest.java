/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.techcode.logbulk.net;

import io.vertx.core.buffer.Buffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

/**
 * Test for packet codec.
 */
public class PacketCodecTest {

    @Test public void testTransform1() {
        // Prepare mocks
        Packet mockedPacket = mock(Packet.class);

        // Test
        PacketCodec codec = new PacketCodec();
        codec.transform(mockedPacket);
        verify(mockedPacket, never()).copy();
    }

    @Test public void testTransform2() {
        // Prepare mocks
        Packet mockedPacket = mock(Packet.class);

        // Test
        PacketCodec codec = new PacketCodec();
        Packet transformed = codec.transform(mockedPacket);
        assertEquals(mockedPacket, transformed);
    }

    @Test public void testDecode() {
        PacketCodec codec = new PacketCodec();
        codec.decodeFromWire(0, Buffer.buffer());
        assertNull(codec.decodeFromWire(0, Buffer.buffer()));
    }

    @Test public void testEncode() {
        // Prepare mocks
        Packet mockedPacket = mock(Packet.class);

        // Test
        PacketCodec codec = new PacketCodec();
        Buffer buf = Buffer.buffer();
        codec.encodeToWire(buf, mockedPacket);
        assertEquals(0, buf.length());
    }

    @Test public void testName() {
        // Test
        assertEquals("PacketCodec", new PacketCodec().name());
    }

    @Test public void testSystemCodecID() {
        // Test
        assertEquals(-1, new PacketCodec().systemCodecID());
    }

}
