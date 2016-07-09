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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.techcode.logbulk.component.ComponentVerticle;
import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

/**
 * Csv transformer pipeline component.
 */
@Slf4j
public class CsvTransform extends ComponentVerticle {

    @Override public void start() {
        super.start();

        // Setup
        String source = config.getString("field");
        String separator = config.getString("separator");
        String delimiter = config.getString("delimiter");
        JsonObject rawColumns = config.getJsonObject("columns");
        Map<Integer, String> columns = Maps.newTreeMap();
        for (String key : rawColumns.fieldNames()) {
            Integer conv = Ints.tryParse(key);
            if (conv != null) columns.put(conv, rawColumns.getString(key));
        }

        // Setup parser
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator(separator);
        settings.getFormat().setDelimiter(delimiter.charAt(0));
        settings.trimValues(true);
        CsvParser parser = new CsvParser(settings);

        // Register endpoint
        getEventBus().<JsonObject>localConsumer(endpoint)
                .handler((ConvertHandler) msg -> {
                    // Process
                    JsonObject evt = event(msg);
                    String field = evt.getString(source);
                    if (field != null) {
                        String[] cols = parser.parseLine(field);
                        if (cols.length >= columns.size()) {
                            for (int key : columns.keySet()) {
                                evt.put(columns.get(key), cols[key]);
                            }
                        }
                    }

                    // Send to the next endpoint
                    forward(msg);
                });
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("field") != null, "The field is required");
        checkState(config.getString("separator") != null, "The separator is required");
        checkState(config.getString("delimiter") != null, "The delimiter is required");
        checkState(config.getJsonObject("columns") != null, "The columns is required");
        return config;
    }

}