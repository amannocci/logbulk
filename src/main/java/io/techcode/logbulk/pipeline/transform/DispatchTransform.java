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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Dispatch transformer pipeline component.
 */
public class DispatchTransform extends BaseComponentVerticle {

    // Dispatch routes
    private List<SimpleDispatch> dispatch = Lists.newArrayList();

    @Override public void start() {
        super.start();

        // Setup
        JsonObject rawDispatch = config.getJsonObject("dispatch");

        // Check routes
        Set<String> cachedRoute = Sets.newHashSet(config.getJsonObject("route").fieldNames());
        for (String d : rawDispatch.fieldNames()) {
            checkState(cachedRoute.contains(d), "The route '" + d + "' doesn't exist");
            JsonObject dispatchRoute = rawDispatch.getJsonObject(d);
            String mode = dispatchRoute.getString("mode");
            if (mode != null) {
                if ("start".equals(mode)) {
                    dispatch.add(new StartDispatch(dispatchRoute.getString("field"), dispatchRoute.getString("match"), d));
                } else {
                    dispatch.add(new ContainsDispatch(dispatchRoute.getString("field"), dispatchRoute.getString("match"), d));
                }
            } else {
                dispatch.add(new SimpleDispatch(d));
            }
        }
    }

    @Override public void handle(JsonObject msg) {
        // Process
        dispatch.forEach(d -> d.dispatch(msg));

        // Send to the next endpoint
        forwardAndRelease(msg);
    }

    @Override public void checkConfig(JsonObject config) {
        checkState(config.getJsonObject("dispatch") != null, "The routes is required");
    }

    /**
     * Simple dispatch implementation.
     */
    private class SimpleDispatch {

        // Routes
        private String route;

        /**
         * Create a new simple dispatch.
         *
         * @param route route dispatching.
         */
        public SimpleDispatch(String route) {
            checkNotNull(route, "The route can't be null");
            this.route = route;
        }

        /**
         * Dispatch the body.
         *
         * @param msg message involved.
         */
        public void dispatch(JsonObject msg) {
            forwardAndRelease(updateRoute(msg.copy(), route));
        }

    }

    /**
     * Start dispatch implementation.
     */
    private class StartDispatch extends SimpleDispatch {

        // Field to match
        private String field;

        // Pattern to match
        private String match;

        /**
         * Create a new start dispatch.
         *
         * @param field field to match.
         * @param match pattern to match.
         * @param route route dispatching.
         */
        public StartDispatch(String field, String match, String route) {
            super(route);
            this.field = field;
            this.match = match;
        }

        @Override public void dispatch(JsonObject msg) {
            String value = body(msg).getString(field);
            if (value.startsWith(match)) {
                super.dispatch(msg);
            }
        }
    }

    /**
     * Contains dispatch implementation.
     */
    private class ContainsDispatch extends SimpleDispatch {

        // Field to match
        private String field;

        // Pattern to match
        private String match;

        /**
         * Create a new contains dispatch.
         *
         * @param field field to match.
         * @param match pattern to match.
         * @param route route dispatching.
         */
        public ContainsDispatch(String field, String match, String route) {
            super(route);
            this.field = field;
            this.match = match;
        }

        @Override public void dispatch(JsonObject msg) {
            String value = body(msg).getString(field);
            if (value.contains(match)) {
                super.dispatch(msg);
            }
        }
    }

}
