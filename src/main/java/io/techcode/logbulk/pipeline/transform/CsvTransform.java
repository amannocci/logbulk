/*
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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Csv transformer pipeline component.
 */
public class CsvTransform extends BaseComponentVerticle {

    // Settings
    private String source;
    private CsvParser parser;
    private Map<Integer, String> columns;
    private boolean nullable;

    @Override public void start() {
        super.start();

        // Setup
        source = config.getString("field");
        nullable = config.getBoolean("nullable", true);
        String separator = config.getString("separator");
        String delimiter = config.getString("delimiter");
        JsonObject rawColumns = config.getJsonObject("columns");
        columns = Maps.newTreeMap();
        for (String key : rawColumns.fieldNames()) {
            Integer conv = Ints.tryParse(key);
            if (conv != null) columns.put(conv, rawColumns.getString(key));
        }

        // Setup parser
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator(separator);
        settings.getFormat().setDelimiter(delimiter.charAt(0));
        settings.trimValues(true);
        parser = new CsvParser(settings);

        // Ready
        resume();
    }

    @Override public void handle(JsonObject msg) {
        // Process
        JsonObject body = body(msg);
        String field = body.getString(source);
        if (field != null) {
            String[] cols = parser.parseLine(field);
            if (cols.length >= columns.size()) {
                for (int key : columns.keySet()) {
                    String col = cols[key];
                    if (col == null) {
                        if (nullable) body.put(columns.get(key), cols[key]);
                    } else {
                        body.put(columns.get(key), cols[key]);
                    }
                }
            }
        }

        // Send to the next endpoint
        forwardAndRelease(msg);
    }

    @Override protected void checkConfig(JsonObject config) {
        checkState(config.getString("field") != null, "The field is required");
        checkState(config.getString("separator") != null, "The separator is required");
        checkState(config.getString("delimiter") != null, "The delimiter is required");
        checkState(config.getJsonObject("columns") != null, "The columns is required");
    }

}