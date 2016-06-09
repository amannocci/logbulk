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
package io.techcode.logbulk.component;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.techcode.logbulk.util.HeaderHandler;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.VoidHandler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Mailbox implementation.
 */
public class Mailbox implements Handler<Message<JsonObject>> {

    // Default threehold
    public final static int DEFAULT_THREEHOLD = 1000;

    // Mailbox handler
    private final VoidHandler MAILBOX_HANDLER = new VoidHandler() {
        @Override protected void handle() {
            if (pending.size() > threehold) {
                for (Iterator<Message<JsonObject>> it = pending.iterator(); it.hasNext(); ) {
                    Message<JsonObject> evt = it.next();
                    handler.handle(evt.headers(), evt.body());
                    it.remove();
                }
                limited.forEach(s -> verticle.resume(s, Mailbox.this.endpoint));
                limited.clear();
            } else {
                Message<JsonObject> evt = pending.poll();
                handler.handle(evt.headers(), evt.body());
            }
        }
    };

    // Component verticle owner
    private ComponentVerticle verticle;

    // Endpoint
    private String endpoint;

    // Treehold
    private int threehold;

    private HeaderHandler<MultiMap, JsonObject> handler;

    // Context
    private Context ctx;

    // Pending message to process
    private Queue<Message<JsonObject>> pending = Lists.newLinkedList();

    // Limited source
    private Set<String> limited = Sets.newHashSet();

    /**
     * Create a new mailbox.
     *
     * @param verticle  verticle owning the mailbox.
     * @param threehold threehold of the mailbox.
     * @param handler   component logic.
     */
    public Mailbox(ComponentVerticle verticle, String endpoint, int threehold, HeaderHandler<MultiMap, JsonObject> handler) {
        checkNotNull(verticle, "The verticle can't be null");
        checkArgument(!Strings.isNullOrEmpty(endpoint), "The endpoint can't be null or empty");
        checkArgument(threehold > 0, "The threehold can't be inferior to 1");
        checkNotNull(handler, "The handle can't be null");
        this.verticle = verticle;
        this.endpoint = endpoint + ".mailbox." + verticle.getUuid();
        this.threehold = threehold;
        this.handler = handler;
        this.ctx = verticle.getVertx().getOrCreateContext();
    }

    @Override public void handle(Message<JsonObject> event) {
        pending.add(event);
        if (pending.size() > threehold && !limited.contains(verticle.source(event.headers()))) {
            String source = verticle.source(event.headers());
            verticle.pause(source, endpoint);
            limited.add(source);
        }
        ctx.runOnContext(MAILBOX_HANDLER);
    }

}