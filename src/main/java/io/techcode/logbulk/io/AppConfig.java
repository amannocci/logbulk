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
    public static final String INPUT = "input";
    public static final String OUTPUT = "output";
    public static final String TRANSFORM = "transform";
    public static final String HAS_MAILBOX = "hasMailbox";
    public static final String MAILBOX = "mailbox";
    public static final String ROUTE = "route";
    public static final String ENDPOINT = "endpoint";
    public static final String FIFO = "fifo";
    public static final String INSTANCE = "instance";
    public static final String SETTINGS = "settings";
    public static final String WORKER = "worker";
    public static final String STATUS = "status";
    public static final String SETTING = "setting";
    public static final String COMPONENT = "component";
    public static final String IDLE = "idle";
    public static final String THRESHOLD = "threshold";

    // Configuration wrapped
    private final Config config;

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
        return block(SETTING);
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
        return block(ROUTE);
    }

    /**
     * Gets a section route.
     *
     * @param key section key.
     * @return section.
     */
    private Set<Map.Entry<String, ConfigValue>> section(String key) {
        return config.getObject(key).entrySet();
    }

    /**
     * Gets a block route.
     *
     * @param key block key.
     * @return block.
     */
    private JsonObject block(String key) {
        return new Configuration(config.getConfig(key).root().render(ConfigRenderOptions.concise().setOriginComments(false).setJson(true)));
    }

    @Override public String toString() {
        return config.root().render(ConfigRenderOptions.concise().setOriginComments(false).setJson(false));
    }

}