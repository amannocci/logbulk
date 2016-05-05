/**
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016
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
package io.techcode.logbulk;

import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.techcode.logbulk.component.ComponentRegistry;
import io.techcode.logbulk.io.AppConfig;
import io.techcode.logbulk.io.FastJsonObjectMessageCodec;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Set;

/**
 * Logbulk is an high-performance log processor.
 */
@Slf4j
public class Logbulk extends AbstractVerticle {

    // Application configuration
    private AppConfig config;

    // Component registry
    private ComponentRegistry registry;

    @Override public void start() {
        // Load configuration
        config = new AppConfig();

        // Register all components
        registry = new ComponentRegistry(this);
        registry.registerAll(config);

        // Register custom codecs
        vertx.eventBus().registerCodec(new FastJsonObjectMessageCodec());

        // Deploy all outputs components
        setups("output", config.outputs());

        // Deploy all transforms components
        setups("transform", config.transforms());

        // Deploy all inputs components
        setups("input", config.inputs());
    }

    @Override public void stop() {
        log.info("Logbulk is shutting down...");
    }

    /**
     * Setup all components in a section.
     *
     * @param section section to analyze.
     * @param entries details of setup.
     */
    private void setups(String section, Set<Map.Entry<String, ConfigValue>> entries) {
        for (Map.Entry<String, ConfigValue> el : entries) {
            // Extract json configuration
            DeploymentOptions deployment = new DeploymentOptions();
            JsonObject conf = new JsonObject(el.getValue().render(ConfigRenderOptions.concise().setJson(true)));

            // Handle special case
            conf.put("source", conf.getString("route"));
            if ("transform".equals(section)) deployment.setInstances(conf.getInteger("instance", 1));
            conf.put("route", config.routes());

            // Handle generic case
            if (conf.getBoolean("worker", false)) deployment.setWorker(true);

            // Map configuration & deploy
            deployment.setConfig(conf);
            int idx = el.getKey().indexOf('-');
            String type = (idx != -1) ? el.getKey().substring(0, idx) : el.getKey();
            vertx.deployVerticle(registry.getComponent(section + '.' + type), deployment);
        }
    }

}