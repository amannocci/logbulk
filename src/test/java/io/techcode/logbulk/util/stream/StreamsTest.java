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
package io.techcode.logbulk.util.stream;

import io.techcode.logbulk.util.stream.Streams;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Test for Streams.
 */
public class StreamsTest {

    private Stream<Object> untypedStream;

    @Before public void setUp() {
        untypedStream = Stream.of("foo", 1, 2L, 2.0F, "bar");
    }

    @Test public void testTo1() throws Exception {
        List<String> typed = Streams.to(untypedStream, String.class).collect(Collectors.toList());
        assertEquals(2, typed.size());
    }

    @Test public void testTo2() throws Exception {
        List<Integer> typed = Streams.to(untypedStream, Integer.class).collect(Collectors.toList());
        assertEquals(1, typed.size());
    }

    @Test public void testTo3() throws Exception {
        List<Long> typed = Streams.to(untypedStream, Long.class).collect(Collectors.toList());
        assertEquals(1, typed.size());
    }

    @Test public void testTo4() throws Exception {
        List<Float> typed = Streams.to(untypedStream, Float.class).collect(Collectors.toList());
        assertEquals(1, typed.size());
    }

    @Test public void testConcat() throws Exception {
        List<String> typed = Streams.concat(Arrays.asList(
                Stream.of("test0"),
                Stream.of("test1"),
                Stream.of("test2"),
                Stream.of("test3", "test4")
        )).collect(Collectors.toList());
        assertEquals(5, typed.size());
    }

}
