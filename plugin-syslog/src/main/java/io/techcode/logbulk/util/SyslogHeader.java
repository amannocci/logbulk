/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2017
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
package io.techcode.logbulk.util;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Syslog headers.
 */
public enum SyslogHeader {
    FACILITY,
    SEVERITY,
    SEVERITY_TEXT,
    TIMESTAMP,
    HOST,
    APPLICATION,
    PROCESSUS,
    MESSAGE,
    ID,
    VERSION,
    DATA;


    private static Map<String, SyslogHeader> BY_NAME = Maps.newHashMap();

    static {
        for (SyslogHeader header : values()) {
            BY_NAME.put(header.name().toLowerCase(), header);
        }
    }

    /**
     * Retrieve a syslog header by name.
     *
     * @param name name of the syslog header.
     * @return syslog header.
     */
    public static SyslogHeader byName(String name) {
        return BY_NAME.get(name);
    }
}