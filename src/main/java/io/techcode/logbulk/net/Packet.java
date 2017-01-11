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
package io.techcode.logbulk.net;

import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/**
 * EventBus packet message.
 */
@Data
@Builder
public class Packet {

    @NonNull private Header header;
    @NonNull private JsonObject body;

    @Data
    @Builder
    @EqualsAndHashCode(callSuper = true)
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

        public Header copy() {
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
    }

    public Packet copy() {
        return Packet.builder()
                .header(header.copy())
                .body(body.copy())
                .build();
    }

}