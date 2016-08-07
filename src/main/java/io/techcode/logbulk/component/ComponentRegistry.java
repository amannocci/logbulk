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
import io.techcode.logbulk.io.AppConfig;
import io.vertx.core.Verticle;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class represent a component registry.
 */
@Slf4j
public class ComponentRegistry {

    // Verticle
    private Verticle verticle;

    // Registry
    private Map<String, String> registry = Maps.newHashMap();

    /**
     * Create a new component registry.
     */
    public ComponentRegistry(Verticle verticle) {
        this.verticle = checkNotNull(verticle, "The verticle can't be null");
    }

    /**
     * Register all components in configuration.
     *
     * @param config application configuration.
     */
    public void registerAll(AppConfig config) {
        checkNotNull(config, "The configuration can't be null");
        for (Map.Entry<String, ConfigValue> entry : config.components()) {
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