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
package io.techcode.logbulk;

import io.techcode.logbulk.component.Mailbox;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Vertx test base.
 */
public abstract class VertxTestBase {

    // Defautl ref
    protected Vertx vertx;

    @Before public void before(TestContext ctx) {
        // Create base vertx
        vertx = Vertx.vertx();

        // Register the context exception handler
        vertx.exceptionHandler(ctx.exceptionHandler());
    }

    @After public void after(TestContext ctx) {
        vertx.close(ctx.asyncAssertSuccess());
    }

    @Test public void testDeployWithoutConf(TestContext ctx) {
        vertx.deployVerticle(getVerticle().getName(), ctx.asyncAssertFailure());
    }

    @Test public void testDeploy(TestContext ctx) {
        vertx.deployVerticle(getVerticle().getName(), new DeploymentOptions().setConfig(conf()), ctx.asyncAssertSuccess());
    }

    /**
     * Returns verticle class to deploy.
     *
     * @return verticle class to deploy.
     */
    protected abstract Class getVerticle();

    /**
     * Returns a valid configuration used during deploy.
     *
     * @return a valid configuration used during deploy.
     */
    protected JsonObject conf() {
        JsonObject conf = new JsonObject();
        conf.put("route", new JsonObject().put("test", new JsonArray().add("test")));
        conf.put("instance", 1);
        conf.put("endpoint", "test");
        conf.put("hasMailbox", false);
        conf.put("mailbox", Mailbox.DEFAULT_THREEHOLD);
        return conf;
    }

}