/*
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2016-2017
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
import io.techcode.logbulk.net.FastJsonArrayCodec;
import io.techcode.logbulk.net.FastJsonObjectCodec;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.net.PacketCodec;
import io.techcode.logbulk.util.StatusMonitor;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Logbulk is an high-performance log processor.
 */
public class Logbulk extends AbstractVerticle {

    // Logging
    private final Logger log = LoggerFactory.getLogger(getClass());

    // Application configuration
    @Getter private AppConfig config;

    // Component registry
    private ComponentRegistry registry;

    // Status monitor
    private StatusMonitor monitor;

    @Override public void start(Future<Void> startFuture) {
        // Ensure error are handle correctly
        vertx.exceptionHandler(log::error);

        // Load configuration
        config = new AppConfig();

        // Register all components
        registry = new ComponentRegistry(this);

        // Register custom codecs
        vertx.eventBus()
                .registerDefaultCodec(Packet.class, new PacketCodec())

                // Replace system implementation when used with delivery options
                .registerCodec(new FastJsonObjectCodec())
                .registerCodec(new FastJsonArrayCodec());

        // Setup status monitor
        monitor = new StatusMonitor(vertx, config.settings().getLong(AppConfig.STATUS, -1L));

        // Deploy all outputs & transforms components and input
        CompositeFuture.all(
                setups(AppConfig.OUTPUT, config.outputs()),
                setups(AppConfig.TRANSFORM, config.transforms())
        ).setHandler(h ->
                setups(AppConfig.INPUT, config.inputs()).setHandler(e -> {
                    // Release monitor if uneeded
                    monitor = null;

                    // Notify
                    startFuture.complete();
                })
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
            Configuration conf = new Configuration(el.getValue().render(ConfigRenderOptions.concise().setJson(true)));
            int instance = conf.getInteger(AppConfig.INSTANCE, 1);
            String endpoint = el.getKey();

            // Handle special case
            conf.put(AppConfig.ENDPOINT, endpoint);
            conf.put(AppConfig.HAS_MAILBOX, !AppConfig.INPUT.equals(section));
            conf.put(AppConfig.SETTING, config.settings());
            conf.put(AppConfig.ROUTE, config.routes());
            if (!AppConfig.INPUT.equals(section)) {
                deployment.setInstances(instance);
            }

            // Handle generic case
            if (conf.getBoolean(AppConfig.WORKER, false)) {
                deployment.setWorker(true);
            }

            // Map configuration & deploy
            Future completion = Future.future();
            completions.add(completion);
            Handler<AsyncResult<String>> deploy = new ComponentDeployment(deployment, endpoint, completion, section, conf);

            // Deploy mailbox first
            if (conf.getBoolean(AppConfig.HAS_MAILBOX)) {
                // Add to monitoring if needed
                if (monitor.isEnable()) {
                    monitor.addMailbox(endpoint);
                }

                // Create configuration and deploy
                JsonObject mailboxConf = new JsonObject();
                mailboxConf.put(AppConfig.ROUTE, conf.getJsonObject(AppConfig.ROUTE));
                mailboxConf.put(AppConfig.INSTANCE, instance);
                mailboxConf.put(AppConfig.ENDPOINT, endpoint);
                mailboxConf.put(AppConfig.HAS_MAILBOX, false);
                mailboxConf.put(AppConfig.FIFO, conf.getBoolean(AppConfig.FIFO, true));
                mailboxConf.put(AppConfig.MAILBOX, conf.getInteger(AppConfig.MAILBOX, Mailbox.DEFAULT_THRESHOLD));
                vertx.deployVerticle(Mailbox.class.getName(), new DeploymentOptions().setConfig(mailboxConf), deploy);
            } else {
                deploy.handle(null);
            }
        }

        // Compose all futures
        return CompositeFuture.all(completions);
    }

    /**
     * Component deployement implementation.
     */
    @AllArgsConstructor
    private class ComponentDeployment implements Handler<AsyncResult<String>> {

        private DeploymentOptions deployment;
        private String endpoint;
        private Future completion;
        private String section;
        private JsonObject conf;

        @Override public void handle(AsyncResult<String> result) {
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

}