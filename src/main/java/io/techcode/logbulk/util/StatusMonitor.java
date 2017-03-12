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
package io.techcode.logbulk.util;

import com.englishtown.promises.Promise;
import com.englishtown.promises.When;
import com.englishtown.promises.WhenFactory;
import com.englishtown.vertx.promises.WhenEventBus;
import com.englishtown.vertx.promises.impl.DefaultWhenEventBus;
import com.englishtown.vertx.promises.impl.VertxExecutor;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Status monitor implementation.
 */
public class StatusMonitor {

    // Logger
    private static final Logger log = LoggerFactory.getLogger(StatusMonitor.class);

    // Mailboxs
    private Set<String> mailboxs = Sets.newTreeSet();

    // Periodic task
    private long time;

    /**
     * Create a new status monitor.
     *
     * @param vertx vertx instance.
     * @param time  time before each check.
     */
    public StatusMonitor(Vertx vertx, long time) {
        this.time = time;
        if (isEnable()) {
            // Create a promise event bus
            VertxExecutor executor = new VertxExecutor(vertx);
            When when = WhenFactory.createFor(() -> executor);
            WhenEventBus eventBus = new DefaultWhenEventBus(vertx, when);

            // Setup periodic task
            vertx.setPeriodic(time, h -> {
                // Convert mailbox to promise
                List<Promise<Message<JsonObject>>> promises = Lists.newArrayListWithCapacity(mailboxs.size());
                mailboxs.forEach(mailbox -> promises.add(eventBus.send(mailbox + ".status", new JsonObject())));

                // Send all promise
                when.all(promises).then(
                        replies -> {
                            JsonObject status = new JsonObject();
                            replies.forEach(r -> status.mergeIn(r.body()));
                            log.info(status.put("type", "monitor"));
                            return null;
                        },
                        err -> {
                            log.error("Can't handle status:", err);
                            return null;
                        });
            });
        }
    }

    /**
     * Returns true if the monitor is enable.
     *
     * @return true if the monitor is enable, otherwise false.
     */
    public boolean isEnable() {
        return time > 0;
    }

    /**
     * Add a mailbox to monitoring.
     *
     * @param mailbox mailbox endpoint.
     */
    public void addMailbox(String mailbox) {
        mailboxs.add(mailbox);
    }

    /**
     * Remove a mailbox from monitoring.
     *
     * @param mailbox mailbox endpoint.
     */
    public void removeMailbox(String mailbox) {
        mailboxs.remove(mailbox);
    }

}