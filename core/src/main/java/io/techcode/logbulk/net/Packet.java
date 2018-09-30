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
package io.techcode.logbulk.net;

import io.vertx.core.json.JsonObject;
import lombok.*;

import java.util.Objects;

/**
 * EventBus packet message.
 */
@Getter
@Setter
@Builder
@ToString
public class Packet {

    @NonNull private Header header;
    @NonNull private JsonObject body;

    public Packet copy() {
        return Packet.builder()
                .header(header.copy())
                .body(body.copy())
                .build();
    }

    @Override public final boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Packet)) return false;
        Packet packet = (Packet) o;
        return Objects.equals(header, packet.header) &&
                Objects.equals(body, packet.body);
    }

    @Override public final int hashCode() {
        return Objects.hash(header, body);
    }

    @Getter
    @Setter
    @Builder
    @ToString
    public static class Header extends JsonObject {
        @NonNull private String source;
        @NonNull private String route;
        private String oldRoute;
        private int previous;
        private int current;

        /**
         * Ensure default values.
         */
        public static class HeaderBuilder {
            protected int previous = -1;
            protected int current = 0;
        }

        @Override public Header copy() {
            Header cpy = Header.builder()
                    .source(source)
                    .route(route)
                    .oldRoute(oldRoute)
                    .previous(previous)
                    .current(current)
                    .build();
            cpy.getMap().putAll(super.copy().getMap());
            return cpy;
        }

        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Header)) return false;
            if (!super.equals(o)) return false;
            Header entries = (Header) o;
            return previous == entries.previous &&
                    current == entries.current &&
                    Objects.equals(source, entries.source) &&
                    Objects.equals(route, entries.route) &&
                    Objects.equals(oldRoute, entries.oldRoute);
        }

        @Override public int hashCode() {
            return Objects.hash(super.hashCode(), source, route, oldRoute, previous, current);
        }

    }

}