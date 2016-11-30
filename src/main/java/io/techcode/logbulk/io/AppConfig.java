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
package io.techcode.logbulk.io;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.vertx.core.json.JsonObject;

import java.util.Map;
import java.util.Set;

/**
 * This class is a app configuration container.
 */
public class AppConfig {

    // Default application reference
    public static final Config REFERENCE = ConfigFactory.defaultReference();

    // All configuration keys
    private static final String SETTING = "setting";
    private static final String COMPONENT = "component";
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TRANSFORM = "transform";
    private static final String ROUTE = "route";

    // Configuration wrapped
    private Config config;

    /**
     * Create a new app config.
     */
    public AppConfig() {
        this.config = ConfigFactory.load().withFallback(REFERENCE);
    }

    /**
     * Gets the component section.
     *
     * @return component section.
     */
    public Set<Map.Entry<String, ConfigValue>> components() {
        return section(COMPONENT);
    }

    /**
     * Gets the setting section.
     *
     * @return setting section.
     */
    public JsonObject settings() {
        return new Configuration(new JsonObject(config.getConfig(SETTING).root().render(ConfigRenderOptions.concise().setOriginComments(false).setJson(true))));
    }

    /**
     * Gets the input section.
     *
     * @return input section.
     */
    public Set<Map.Entry<String, ConfigValue>> inputs() {
        return section(INPUT);
    }

    /**
     * Gets the output section.
     *
     * @return output section.
     */
    public Set<Map.Entry<String, ConfigValue>> outputs() {
        return section(OUTPUT);
    }

    /**
     * Gets the transform section.
     *
     * @return transform section.
     */
    public Set<Map.Entry<String, ConfigValue>> transforms() {
        return section(TRANSFORM);
    }

    /**
     * Gets the route section.
     *
     * @return route section.
     */
    public JsonObject routes() {
        return new JsonObject(config.getConfig(ROUTE).root().render(ConfigRenderOptions.concise().setOriginComments(false).setJson(true)));
    }

    /**
     * Gets a section.route
     *
     * @return section.
     */
    private Set<Map.Entry<String, ConfigValue>> section(String key) {
        return config.getObject(key).entrySet();
    }

    @Override public String toString() {
        return config.root().render(ConfigRenderOptions.concise().setOriginComments(false).setJson(false));
    }

}