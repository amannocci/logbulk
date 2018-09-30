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
package io.techcode.logbulk.pipeline.input;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.techcode.logbulk.util.SyslogHeader;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.parsetools.RecordParser;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Syslog input pipeline component.
 */
public class SyslogInput extends TcpInput {

    // Constant for reader
    private static final char NILVALUE = '-';
    private static final char SPACE = ' ';

    // Some constants
    private static final String CONF_FRAMING = "framing";
    private static final String CONF_MAPPING = "mapping";
    private static final String CONF_DELIMITER = "delimiter";

    // Settings
    private Map<SyslogHeader, String> mapping;
    private boolean skipStructuredData;

    @Override protected void onStart() {
        // Setup mapping
        JsonObject map = config.getJsonObject(CONF_MAPPING, new JsonObject());
        mapping = Maps.newEnumMap(SyslogHeader.class);
        for (String entry : map.fieldNames()) {
            mapping.put(SyslogHeader.byName(entry), map.getString(entry));
        }
        skipStructuredData = config.getBoolean("skipStructuredData", false);
    }

    @Override protected Handler<Buffer> decoder() {
        // Setup framing
        String framing = config.getString(CONF_FRAMING);
        if ("delimited".equals(framing)) {
            return RecordParser.newDelimited(config.getString(CONF_DELIMITER), this::decode);
        } else {
            return new Handler<Buffer>() {
                private int pos = 0;
                private int recordSize = 0;
                private Buffer buf = Buffer.buffer();

                @Override public void handle(Buffer evt) {
                    buf.appendBuffer(evt);

                    // Current length of buffer
                    int len = buf.length() - pos;
                    while (len >= recordSize) {
                        // Attempt to read pos
                        if (recordSize == 0) {
                            while (pos < len) {
                                if ((buf.getByte(pos) & 0xFF) == ' ') {
                                    recordSize = Ints.tryParse(buf.getString(0, pos));
                                    pos += 1;
                                    len = buf.length() - pos;
                                    break;
                                } else {
                                    pos += 1;
                                }
                            }
                        }

                        // We don't know length
                        if (recordSize == 0) return;

                        // Parse
                        if (len >= recordSize) {
                            decode(buf.getBuffer(pos, pos + recordSize));
                            buf = buf.getBuffer(pos + recordSize, buf.length());
                            pos = 0;
                            recordSize = 0;
                            len = buf.length();
                        }
                    }
                }
            };
        }
    }

    /**
     * Decode a syslog message from buffer.
     *
     * @param buf buffer involved.
     */
    private void decode(Buffer buf) {
        JsonObject doc = new JsonObject();
        Reader reader = new Reader(buf);

        // Extract priority
        reader.expect('<');
        int priority = reader.readInt();
        reader.expect('>');

        // Extract version
        int version = reader.readInt();
        reader.expect(SPACE);

        // Extract timestamp
        Object timestamp = getTimestamp(reader);

        // Extract all identifiers
        String host = reader.getIdentifier();
        String app = reader.getIdentifier();
        String procId = reader.getIdentifier();
        String msgId = reader.getIdentifier();

        // Extract data
        Object structuredData = (skipStructuredData) ? null : getStructuredData(reader);

        // Extract message
        String message;
        if (reader.is(SPACE)) {
            reader.getc();
            message = reader.rest();
        } else {
            message = reader.rest();
        }

        // Convert
        int severity = priority & 0x7;
        int facility = priority >> 3;

        // Populate
        if (mapping.containsKey(SyslogHeader.FACILITY)) {
            doc.put(mapping.get(SyslogHeader.FACILITY), facility);
        }
        if (mapping.containsKey(SyslogHeader.SEVERITY)) {
            doc.put(mapping.get(SyslogHeader.SEVERITY), severity);
        }
        if (mapping.containsKey(SyslogHeader.SEVERITY_TEXT)) {
            doc.put(mapping.get(SyslogHeader.SEVERITY_TEXT), Severity.parseInt(severity).getLabel());
        }
        if (timestamp != null && mapping.containsKey(SyslogHeader.TIMESTAMP)) {
            doc.put(mapping.get(SyslogHeader.TIMESTAMP), timestamp);
        }
        if (host != null && mapping.containsKey(SyslogHeader.HOST)) {
            doc.put(mapping.get(SyslogHeader.HOST), host);
        }
        if (app != null && mapping.containsKey(SyslogHeader.APPLICATION)) {
            doc.put(mapping.get(SyslogHeader.APPLICATION), app);
        }
        if (procId != null && mapping.containsKey(SyslogHeader.PROCESSUS)) {
            doc.put(mapping.get(SyslogHeader.PROCESSUS), procId);
        }
        if (msgId != null && mapping.containsKey(SyslogHeader.ID)) {
            doc.put(mapping.get(SyslogHeader.ID), msgId);
        }
        if (mapping.containsKey(SyslogHeader.VERSION)) {
            doc.put(mapping.get(SyslogHeader.VERSION), version);
        }
        if (structuredData != null && mapping.containsKey(SyslogHeader.DATA)) {
            doc.put(mapping.get(SyslogHeader.DATA), structuredData);
        }
        if (mapping.containsKey(SyslogHeader.MESSAGE)) {
            doc.put(mapping.get(SyslogHeader.MESSAGE), message);
        }

        // Send message
        if (!doc.isEmpty()) {
            createEvent(doc);
        }
    }


    /**
     * Default implementation returns the date as a String (if present).
     *
     * @param reader the reader.
     * @return the timestamp.
     */
    private String getTimestamp(Reader reader) {
        int c = reader.getc();

        if (c == NILVALUE) {
            return null;
        }

        if (!Character.isDigit(c)) {
            throw new IllegalStateException("Year expected @" + reader.idx);
        }

        StringBuilder dateBuilder = new StringBuilder(64);
        dateBuilder.append((char) c);
        while ((c = reader.getc()) != SPACE) {
            dateBuilder.append((char) c);
        }
        return dateBuilder.toString();
    }

    /**
     * Extract structured data.
     *
     * @param reader the reader.
     * @return structured data.
     */
    private JsonArray getStructuredData(Reader reader) {
        if (reader.is(NILVALUE)) {
            reader.getc();
            return null;
        }
        return parseStructuredDataElements(reader);
    }

    /**
     * Default implementation returns a list of structured data elements with
     * no internal parsing.
     *
     * @param reader the reader.
     * @return the structured data.
     */
    private JsonArray parseStructuredDataElements(Reader reader) {
        JsonArray fragments = new JsonArray();
        while (reader.is('[')) {
            reader.mark();
            reader.skipTo(']');
            fragments.add(reader.getMarkedSegment());
        }
        return fragments;
    }

    /**
     * Buffer reader.
     */
    protected static class Reader {

        // Line buffer
        private final Buffer line;

        // Current index & mark
        private int idx;
        private int mark;

        /**
         * Create a new reader.
         *
         * @param buf line buffer.
         */
        Reader(Buffer buf) {
            this.line = buf;
        }

        /**
         * Mark current index.
         */
        void mark() {
            mark = idx;
        }

        /**
         * Get a char from position.
         *
         * @param pos position.
         * @return char.
         */
        private char getChar(int pos) {
            return (char) (line.getByte(pos) & 0xFF);
        }

        /**
         * Get marked segment.
         *
         * @return marked segment.
         */
        String getMarkedSegment() {
            checkState(mark <= idx, "mark is greater than this.idx");
            return this.line.getString(mark, idx);
        }

        /**
         * Get current char.
         *
         * @return current char.
         */
        int current() {
            return getChar(idx);
        }

        /**
         * Get previous char.
         *
         * @return previous char.
         */
        int prev() {
            return getChar(idx - 1);
        }

        /**
         * Get next char.
         *
         * @return next char.
         */
        int next() {
            return getChar(idx + 1);
        }

        /**
         * Get a char and advance.
         *
         * @return char.
         */
        int getc() {
            return getChar(idx++);
        }

        /**
         * Rollback.
         */
        void ungetc() {
            this.idx--;
        }

        /**
         * Get an int.
         *
         * @return int.
         */
        int getInt() {
            int c = getc();
            if (!Character.isDigit(c)) {
                ungetc();
                return -1;
            }
            return c - '0';
        }

        /**
         * Attempt to read an int.
         *
         * @return int.
         */
        int readInt() {
            int val = 0;
            while (isDigit()) {
                val = (val * 10) + getInt();
            }
            return val;
        }

        /**
         * Returns true if current char is equals.
         *
         * @param ch char to check with.
         * @return true if current char is equals, otherwise false.
         */
        boolean is(char ch) {
            return idx < line.length() && current() == ch;
        }

        /**
         * Returns true if the previous char was equals.
         *
         * @param ch char to check with.
         * @return true if current char was equals, otherwise false.
         */
        boolean was(char ch) {
            return prev() == ch;
        }

        /**
         * Check if the current char is a digit.
         *
         * @return true if the current char is a digit, otherwise false.
         */
        boolean isDigit() {
            return Character.isDigit(current());
        }

        /**
         * Check current character.
         *
         * @param ch char to found.
         */
        void expect(char ch) {
            if (getChar(idx++) != ch) {
                throw new IllegalStateException("Expected '" + ch + "' @" + idx);
            }
        }

        /**
         * Read until char.
         *
         * @param searchChar char to search.
         */
        void skipTo(char searchChar) {
            while (!is(searchChar) || was('\\')) {
                getc();
            }
            getc();
        }

        /**
         * Returns rest of the line.
         *
         * @return rest of the line.
         */
        String rest() {
            if (idx >= line.length()) {
                return StringUtils.EMPTY;
            }

            // Remove trailing char
            char lastCharacter = (char) (line.getByte(line.length() - 1) & 0xFF);
            if (lastCharacter == '\n') {
                return line.getString(idx, line.length() - 1);
            } else {
                return line.getString(idx, line.length());
            }
        }

        /**
         * Gets an identifier.
         *
         * @return an identifier.
         */
        String getIdentifier() {
            StringBuilder sb = new StringBuilder(255);
            int ch;
            while (true) {
                ch = getc();
                if (ch >= 33 && ch <= 127) {
                    sb.append((char) ch);
                } else {
                    break;
                }
            }
            return sb.toString();
        }
    }

    /**
     * All possibles severity.
     */
    protected enum Severity {

        DEBUG(7, "DEBUG"),
        INFO(6, "INFO"),
        NOTICE(5, "NOTICE"),
        WARN(4, "WARN"),
        ERROR(3, "ERRORS"),
        CRITICAL(2, "CRITICAL"),
        ALERT(1, "ALERT"),
        EMERGENCY(0, "EMERGENCY"),
        UNDEFINED(-1, "UNDEFINED");

        @Getter private final int level;
        @Getter private final String label;

        Severity(int level, String label) {
            this.level = level;
            this.label = label;
        }

        public static Severity parseInt(int syslogSeverity) {
            switch (syslogSeverity) {
                case 7:
                    return DEBUG;
                case 6:
                    return INFO;
                case 5:
                    return NOTICE;
                case 4:
                    return WARN;
                case 3:
                    return ERROR;
                case 2:
                    return CRITICAL;
                case 1:
                    return ALERT;
                case 0:
                    return EMERGENCY;
                default:
                    return UNDEFINED;
            }
        }
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("dispatch") != null, "The dispatch is required");
        checkState(config.getInteger("port") != null, "The port is required");
    }

}
