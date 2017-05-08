package io.techcode.logbulk.net;

import io.vertx.core.eventbus.impl.codecs.JsonArrayMessageCodec;
import io.vertx.core.json.JsonArray;

/**
 * Fast json array message codec that avoid copy.
 */
public class FastJsonArrayCodec extends JsonArrayMessageCodec {

    public static final String CODEC_NAME = FastJsonArrayCodec.class.getSimpleName();

    @Override public JsonArray transform(JsonArray evt) {
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