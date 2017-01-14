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
package io.techcode.logbulk.component;

import com.google.common.collect.Maps;
import com.typesafe.config.ConfigValue;
import io.techcode.logbulk.Logbulk;
import io.techcode.logbulk.io.AppConfig;
import io.techcode.logbulk.util.stream.Streams;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.NonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

/**
 * This class represent a component registry.
 */
public class ComponentRegistry {

    // Logging
    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    // Verticle
    private final Logbulk verticle;

    // Registry
    private final Map<String, String> registry = Maps.newHashMap();

    /**
     * Create a new component registry.
     */
    public ComponentRegistry(@NonNull Logbulk verticle) {
        this.verticle = verticle;
        registerAll();
        analyzeRoutes();
    }

    /**
     * Register all components in configuration.
     */
    public void registerAll() {
        for (Map.Entry<String, ConfigValue> entry : verticle.getConfig().components()) {
            for (Map.Entry<String, ConfigValue> el : entry.getValue().atKey(entry.getKey()).entrySet()) {
                try {
                    Class.forName(el.getValue().unwrapped().toString());
                    registry.put(el.getKey(), el.getValue().unwrapped().toString());
                } catch (ClassNotFoundException e) {
                    log.error("Unknown component: " + el.getKey() + " / " + el.getValue().unwrapped());
                    verticle.getVertx().close();
                    return;
                }
            }
        }
    }

    /**
     * Analyze all routes in configuration.
     */
    public void analyzeRoutes() {
        // Configuration
        AppConfig config = verticle.getConfig();

        // Retrieve all defined components
        Set<String> components = Streams.concat(Arrays.asList(
                config.inputs().stream().map(Map.Entry::getKey),
                config.transforms().stream().map(Map.Entry::getKey),
                config.outputs().stream().map(Map.Entry::getKey)
        )).collect(Collectors.toSet());

        // Iterate over components defined in routes
        JsonObject routes = verticle.getConfig().routes();
        List<String> unknowns = routes.fieldNames().stream()
                .flatMap(route -> Streams.to(routes.getJsonArray(route).stream(), String.class))
                .distinct()
                .filter(c -> !components.contains(c))
                .distinct()
                .collect(Collectors.toList());

        // Print
        if (unknowns.size() > 0) {
            unknowns.forEach(c -> log.error("The component '" + c + "' isn't defined"));
            verticle.getVertx().close();
        }
    }

    /**
     * Retrieve a component by his name.
     *
     * @param key key component.
     * @return class mapping.
     */
    public String getComponent(String key) {
        String component = registry.get(key);
        checkState(component != null, "Unknown component: " + key);
        return component;
    }

}