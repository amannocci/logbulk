/*
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

import com.google.common.collect.Lists;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import io.techcode.logbulk.component.ComponentRegistry;
import io.techcode.logbulk.component.Mailbox;
import io.techcode.logbulk.io.AppConfig;
import io.techcode.logbulk.io.Configuration;
import io.techcode.logbulk.io.FastJsonObjectMessageCodec;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Logbulk is an high-performance log processor.
 */
public class Logbulk extends AbstractVerticle {

    // Logging
    private Logger log = LoggerFactory.getLogger(getClass().getName());

    // Application configuration
    @Getter private AppConfig config;

    // Component registry
    private ComponentRegistry registry;

    @Override public void start(Future<Void> startFuture) {
        // Ensure error are handle correctly
        vertx.exceptionHandler(th -> {
            if (!(th instanceof IllegalStateException) || !"Result is already complete: succeeded".equals(th.getMessage())) {
                log.error(th);
            }
        });

        // Load configuration
        config = new AppConfig();

        // Register all components
        registry = new ComponentRegistry(this);

        // Register custom codecs
        vertx.eventBus().registerCodec(new FastJsonObjectMessageCodec());

        // Deploy all outputs & transforms components and input
        CompositeFuture.all(
                setups("output", config.outputs()),
                setups("transform", config.transforms())
        ).setHandler(h ->
                setups("input", config.inputs()).setHandler(e -> startFuture.complete())
        );
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
    private Future<CompositeFuture> setups(String section, Set<Map.Entry<String, ConfigValue>> entries) {
        // Wait for completion
        List<Future> completions = Lists.newArrayListWithCapacity(entries.size());

        // Iterate over each component
        for (Map.Entry<String, ConfigValue> el : entries) {
            // Extract json configuration
            DeploymentOptions deployment = new DeploymentOptions();
            Configuration conf = new Configuration(new JsonObject(el.getValue().render(ConfigRenderOptions.concise().setJson(true))));
            int instance = conf.getInteger("instance", 1);
            String endpoint = el.getKey();

            // Handle special case
            conf.put("endpoint", endpoint);
            if (!"input".equals(section)) deployment.setInstances(instance);
            conf.put("hasMailbox", !"input".equals(section));
            conf.put("route", config.routes());

            // Handle generic case
            if (conf.getBoolean("worker", false)) deployment.setWorker(true);

            // Map configuration & deploy
            Future completion = Future.future();
            completions.add(completion);
            Handler<AsyncResult<String>> deploy = event -> {
                deployment.setConfig(conf);
                String type = type(endpoint);
                vertx.deployVerticle(registry.getComponent(section + '.' + type), deployment, h -> {
                    if (h.failed()) {
                        log.error("Error during component setup:", h.cause());
                        vertx.close();
                    } else {
                        completion.complete();
                    }
                });
            };

            // Deploy mailbox first
            if (conf.getBoolean("hasMailbox")) {
                JsonObject mailboxConf = new JsonObject();
                mailboxConf.put("route", conf.getJsonObject("route"));
                mailboxConf.put("instance", instance);
                mailboxConf.put("endpoint", endpoint);
                mailboxConf.put("hasMailbox", false);
                mailboxConf.put("mailbox", conf.getInteger("mailbox", Mailbox.DEFAULT_THREEHOLD));
                vertx.deployVerticle(Mailbox.class.getName(), new DeploymentOptions().setConfig(mailboxConf), deploy);
            } else {
                deploy.handle(null);
            }
        }

        // Compose all futures
        return CompositeFuture.all(completions);
    }

    /**
     * Gets the type of the component.
     *
     * @param component component id.
     * @return type of the component.
     */
    private String type(String component) {
        int idx = component.indexOf('/');
        return (idx != -1) ? component.substring(0, idx) : component;
    }

}