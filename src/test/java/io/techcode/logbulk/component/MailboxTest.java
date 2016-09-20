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

import io.techcode.logbulk.VertxTestBase;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Test for Mailbox.
 */
@RunWith(VertxUnitRunner.class)
public class MailboxTest extends VertxTestBase {

    @Test public void testDeployWithoutConf(TestContext ctx) {
        vertx.deployVerticle(Mailbox.class.getName(), ctx.asyncAssertFailure());
    }

    @Test public void testDeploy(TestContext ctx) {
        vertx.deployVerticle(Mailbox.class.getName(), new DeploymentOptions().setConfig(conf()), ctx.asyncAssertSuccess());
    }

    /**
     * Create a testing configuration.
     *
     * @return testinng configuration.
     */
    private JsonObject conf() {
        JsonObject conf = new JsonObject();
        conf.put("route", new JsonObject().put("test", new JsonArray().add("test")));
        conf.put("instance", 1);
        conf.put("endpoint", "test");
        conf.put("hasMailbox", false);
        conf.put("mailbox", Mailbox.DEFAULT_THREEHOLD);
        return conf;
    }

}