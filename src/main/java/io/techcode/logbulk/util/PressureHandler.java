/**
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

import com.google.common.collect.Sets;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.streams.ReadStream;

import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Pressure handler implementation.
 */
public class PressureHandler implements Handler<Message<String>> {

    // Back pressure
    private Set<String> nextPressure = Sets.newHashSet();

    // Stream source
    private ReadStream stream;

    // State of the stream
    private boolean paused = false;

    /**
     * Create a new back pressure handler.
     *
     * @param stream stream to handle.
     */
    public PressureHandler(ReadStream stream) {
        checkNotNull(stream, "The stream can't be null");
        this.stream = stream;
    }

    @Override public void handle(Message<String> e) {
        String evt = e.body();
        if (nextPressure.contains(evt)) {
            nextPressure.remove(evt);
        } else {
            nextPressure.add(evt);
        }
        if (paused && nextPressure.isEmpty()) {
            stream.resume();
            paused = false;
        }
        if (!paused && nextPressure.size() > 0) {
            stream.pause();
            paused = true;
        }
    }

}
