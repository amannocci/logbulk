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

import java.util.stream.Stream;

/**
 * Static utility methods pertaining to Stream instances.
 */
public final class Streams {

    // Block constructor
    private Streams() {
    }

    /**
     * Filter to a stream to a specific type of instance and create a new stream of this type.
     *
     * @param stream stream to process.
     * @param type   class type of the new stream.
     * @param <T>    type of the new stream.
     * @return stream newly created.
     */
    @SuppressWarnings("unchecked")
    public static <T> Stream<T> to(Stream<Object> stream, Class<T> type) {
        return stream.filter(type::isInstance).map(e -> (T) e);
    }

    /**
     * Creates a lazily concatenated stream whose elements are all the
     * elements of all streams. The resulting stream is ordered if both
     * of the input streams are ordered, and parallel if either of the input
     * streams is parallel. When the resulting stream is closed, the close
     * handlers for both input streams are invoked.
     *
     * @param <T>     The type of stream elements.
     * @param streams all streams.
     * @return the concatenation of all streams.
     * @implNote Use caution when constructing streams from repeated concatenation.
     * Accessing an element of a deeply concatenated stream can result in deep
     * call chains, or even {@code StackOverflowException}.
     */
    @SuppressWarnings("unchecked")
    public static <T> Stream<T> concat(Iterable<Stream<T>> streams) {
        Stream<T> concat = Stream.empty();
        for (Stream<T> stream : streams) {
            concat = Stream.concat(concat, stream);
        }
        return concat;
    }

}