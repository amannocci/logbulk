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

import io.vertx.core.json.JsonArray;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Test for json array codec.
 */
public class FastJsonArrayCodecTest {

    @Test public void testTransform1() {
        // Prepare mocks
        JsonArray mock = mock(JsonArray.class);

        // Test
        FastJsonArrayCodec codec = new FastJsonArrayCodec();
        codec.transform(mock);
        verify(mock, never()).copy();
    }

    @Test public void testTransform2() {
        // Prepare mocks
        JsonArray mock = mock(JsonArray.class);

        // Test
        FastJsonArrayCodec codec = new FastJsonArrayCodec();
        JsonArray transformed = codec.transform(mock);
        assertEquals(mock, transformed);
    }

    @Test public void testName() {
        // Test
        assertEquals(FastJsonArrayCodec.CODEC_NAME, new FastJsonArrayCodec().name());
    }

    @Test public void testSystemCodecID() {
        // Test
        assertEquals(-1, new FastJsonArrayCodec().systemCodecID());
    }

}
