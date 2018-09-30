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
package io.techcode.logbulk.pipeline.transform;

import com.google.common.base.Strings;
import com.google.common.collect.*;
import io.techcode.logbulk.component.BaseComponentVerticle;
import io.techcode.logbulk.net.Packet;
import io.techcode.logbulk.util.json.JsonPath;
import io.vertx.core.json.JsonObject;
import lombok.NonNull;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

/**
 * Dispatch transformer pipeline component.
 */
public class DispatchTransform extends BaseComponentVerticle {

    // Some constants
    private static final String CONF_FIELD = "field";
    private static final String CONF_MATCH = "match";
    private static final String CONF_DISPATCH = "dispatch";
    private static final String CONF_MISS = "miss";
    private static final String CONF_MODE = "mode";
    private static final String CONF_ROUTE = "route";

    // Dispatch routes
    private final List<Dispatch> dispatch = Lists.newArrayList();

    // Missed dispatching
    private String miss;

    @Override public void start() {
        super.start();

        // Setup
        JsonObject rawDispatch = config.getJsonObject(CONF_DISPATCH);
        miss = config.getString(CONF_MISS);

        // Aggregate equalsIgnoreCase dispatch
        Map<String, EqualsIgnoreCaseDispatch> routing = Maps.newHashMap();

        // Check routes
        Set<String> cachedRoute = Sets.newHashSet(config.getJsonObject(CONF_ROUTE).fieldNames());
        for (String d : rawDispatch.fieldNames()) {
            checkState(cachedRoute.contains(d), "The route '" + d + "' doesn't exist");
            JsonObject dispatchRoute = rawDispatch.getJsonObject(d);
            String mode = dispatchRoute.getString(CONF_MODE);
            if (Strings.isNullOrEmpty(mode)) {
                dispatch.add(new SimpleDispatch(d));
            } else {
                switch (mode) {
                    case "start":
                        dispatch.add(new StartDispatch(dispatchRoute.getString(CONF_FIELD), dispatchRoute.getString(CONF_MATCH), d));
                        break;
                    case "absent":
                        dispatch.add(new AbsentDispatch(dispatchRoute.getString(CONF_FIELD), d));
                        break;
                    case "present":
                        dispatch.add(new PresentDispatch(dispatchRoute.getString(CONF_FIELD), d));
                        break;
                    case "contains":
                        dispatch.add(new ContainsDispatch(dispatchRoute.getString(CONF_FIELD), dispatchRoute.getString(CONF_MATCH), d));
                        break;
                    case "equals":
                        dispatch.add(new EqualsDispatch(dispatchRoute.getString(CONF_FIELD), dispatchRoute.getValue(CONF_MATCH), d));
                        break;
                    case "equalsIgnoreCase": {
                        String field = dispatchRoute.getString(CONF_FIELD);
                        EqualsIgnoreCaseDispatch dispatcher = routing.get(field);
                        if (dispatcher == null) {
                            dispatcher = new EqualsIgnoreCaseDispatch(field);
                            routing.put(field, dispatcher);
                            dispatch.add(dispatcher);
                        }
                        dispatcher.map(dispatchRoute.getString(CONF_MATCH), d);
                    }
                    break;
                    default:
                        log.error("The mode '" + mode + "' isn't support");
                        break;
                }
            }
        }

        // Help gc
        routing.clear();

        // Ready
        resume();
    }

    @Override public void handle(Packet packet) {
        // Process
        boolean routed = false;
        for (Dispatch d : dispatch) {
            routed |= d.dispatch(packet);
        }
        if (miss != null && !routed) {
            send(updateRoute(packet.copy(), miss));
        }

        // Send to the next endpoint
        forwardAndRelease(packet);
    }

    @Override public void checkConfig(JsonObject config) {
        checkState(config.getJsonObject(CONF_DISPATCH) != null, "The routes is required");
    }

    /**
     * Dispatch interface.
     */
    private interface Dispatch {

        /**
         * Dispatch the packet.
         *
         * @param packet packet involved.
         */
        boolean dispatch(Packet packet);

        /**
         * Dispatch the packet to a given route.
         *
         * @param packet packet involved.
         * @param route  route to dispatch.
         */
        boolean dispatch(Packet packet, String route);

    }

    private abstract class AbstractDispatch implements Dispatch {
        @Override public final boolean dispatch(Packet packet, String route) {
            send(updateRoute(packet.copy(), route));
            return true;
        }
    }

    /**
     * Simple dispatch implementation.
     */
    private class SimpleDispatch extends AbstractDispatch {

        // Routes
        private final String route;

        /**
         * Create a new simple dispatch.
         *
         * @param route route dispatching.
         */
        SimpleDispatch(@NonNull String route) {
            this.route = route;
        }

        @Override public boolean dispatch(Packet packet) {
            return dispatch(packet, route);
        }

    }

    /**
     * Present dispatch implementation.
     */
    private class PresentDispatch extends SimpleDispatch {

        // Field to match
        private final JsonPath field;

        /**
         * Create a new router dispatch.
         *
         * @param field field to match.
         * @param route route dispatch.
         */
        PresentDispatch(@NonNull String field, @NonNull String route) {
            super(route);
            this.field = JsonPath.create(field);
        }

        @Override public boolean dispatch(Packet packet) {
            return (field.get(packet.getBody()) != null || field.get(packet.getHeader()) != null) && super.dispatch(packet);
        }

    }

    /**
     * Absent dispatch implementation.
     */
    private class AbsentDispatch extends SimpleDispatch {

        // Field to match
        private final JsonPath field;

        /**
         * Create a new router dispatch.
         *
         * @param field field to match.
         * @param route route dispatch.
         */
        AbsentDispatch(@NonNull String field, @NonNull String route) {
            super(route);
            this.field = JsonPath.create(field);
        }

        @Override public boolean dispatch(Packet packet) {
            return field.get(packet.getBody()) == null && field.get(packet.getHeader()) == null && super.dispatch(packet);
        }

    }


    /**
     * Start dispatch implementation.
     */
    private class StartDispatch extends SimpleDispatch {

        // Field to match
        private final JsonPath field;

        // Pattern to match
        private final String match;

        /**
         * Create a new start dispatch.
         *
         * @param field field to match.
         * @param match pattern to match.
         * @param route route dispatching.
         */
        StartDispatch(@NonNull String field, @NonNull String match, @NonNull String route) {
            super(route);
            this.field = JsonPath.create(field);
            this.match = match;
        }

        @Override public boolean dispatch(Packet packet) {
            String value = field.get(packet.getBody(), String.class);
            return !Strings.isNullOrEmpty(value) && value.startsWith(match) && super.dispatch(packet);
        }
    }

    /**
     * Contains dispatch implementation.
     */
    private class ContainsDispatch extends SimpleDispatch {

        // Field to match
        private final JsonPath field;

        // Pattern to match
        private final String match;

        /**
         * Create a new contains dispatch.
         *
         * @param field field to match.
         * @param match pattern to match.
         * @param route route dispatching.
         */
        ContainsDispatch(@NonNull String field, @NonNull String match, @NonNull String route) {
            super(route);
            this.field = JsonPath.create(field);
            this.match = match;
        }

        @Override public boolean dispatch(Packet packet) {
            String value = field.get(packet.getBody(), String.class);
            return !Strings.isNullOrEmpty(value) && value.contains(match) && super.dispatch(packet);
        }
    }

    /**
     * Equals dispatch implementation.
     */
    private class EqualsDispatch extends SimpleDispatch {

        // Field to match
        private final JsonPath field;

        // Pattern to match
        private final Object match;

        /**
         * Create a new equals dispatch.
         *
         * @param field field to match.
         * @param match pattern to match.
         * @param route route dispatching.
         */
        EqualsDispatch(@NonNull String field, Object match, @NonNull String route) {
            super(route);
            this.field = JsonPath.create(field);
            this.match = match;
        }

        @Override public boolean dispatch(Packet packet) {
            Object value = field.get(packet.getBody());
            return value != null && value.equals(match) && super.dispatch(packet);
        }
    }

    /**
     * Equals ignore case dispatch implementation.
     */
    private class EqualsIgnoreCaseDispatch extends AbstractDispatch {

        // Field to match
        private final JsonPath field;

        // Pattern to match
        private final Multimap<String, String> routing = TreeMultimap.create(String.CASE_INSENSITIVE_ORDER, Ordering.natural());

        /**
         * Create a new equals ignore case dispatch.
         *
         * @param field field to match.
         */
        EqualsIgnoreCaseDispatch(@NonNull String field) {
            this.field = JsonPath.create(field);
        }

        /**
         * Add a mapping based on matching value and routing.
         *
         * @param match value of field to match.
         * @param route route to dispatch.
         */
        void map(String match, String route) {
            routing.put(match, route);
        }

        @Override public boolean dispatch(Packet packet) {
            String value = field.get(packet.getBody(), String.class);
            if (Strings.isNullOrEmpty(value)) {
                return false;
            } else {
                Collection<String> routes = routing.get(value);
                if (routes.isEmpty()) {
                    return false;
                } else {
                    routes.forEach(route -> super.dispatch(packet, route));
                    return true;
                }
            }
        }
    }

}
