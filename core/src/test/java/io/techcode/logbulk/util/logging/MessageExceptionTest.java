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
package io.techcode.logbulk.util.logging;

import org.junit.Test;

import static io.techcode.logbulk.util.Binder.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for MessageException.
 */
public class MessageExceptionTest {

    @Test(expected = NullPointerException.class)
    public void testConstructor1() throws Exception {
        throw new MessageException(null);
    }

    @Test(expected = MessageException.class)
    public void testConstructor2() throws Exception {
        throw new MessageException("foobar");
    }

    @Test public void testGetMessage() throws Exception {
        MessageException exception = new MessageException("foobar");
        assertEquals("foobar", exception.getMessage());
    }

    @Test public void testGetStackTrace() throws Exception {
        MessageException exception = new MessageException("foobar");
        assertEquals(0, exception.getStackTrace().length);
    }

}