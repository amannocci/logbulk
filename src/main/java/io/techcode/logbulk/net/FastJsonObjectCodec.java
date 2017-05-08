package io.techcode.logbulk.net;

import io.vertx.core.eventbus.impl.codecs.JsonObjectMessageCodec;
import io.vertx.core.json.JsonObject;

/**
 * Fast json object message codec that avoid copy.
 */
public class FastJsonObjectCodec extends JsonObjectMessageCodec {

    public static final String CODEC_NAME = FastJsonObjectCodec.class.getSimpleName();

    @Override public JsonObject transform(JsonObject evt) {
        // Avoid copy when we use it as event schema
        return evt;
    }

    @Override public String name() {
        return CODEC_NAME;
    }

    @Override public byte systemCodecID() {
        return -1;
    }

}