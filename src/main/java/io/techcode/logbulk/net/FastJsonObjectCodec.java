package io.techcode.logbulk.net;

import io.vertx.core.eventbus.impl.codecs.JsonObjectMessageCodec;
import io.vertx.core.json.JsonObject;

/**
 * Fast json object message codec that avoid copy.
 */
public class FastJsonObjectCodec extends JsonObjectMessageCodec {

    public static final String NAME = "fastjsonobject";

    @Override public JsonObject transform(JsonObject evt) {
        // Avoid copy when we use it as event schema
        return evt;
    }

    @Override public String name() {
        return NAME;
    }

    @Override public byte systemCodecID() {
        return -1;
    }

}