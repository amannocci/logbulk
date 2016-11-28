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
package io.techcode.logbulk.component;

import io.techcode.logbulk.util.ConvertHandler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Transform component verticle helper.
 */
public abstract class BaseComponentVerticle extends ComponentVerticle implements ConvertHandler {

    // State pause
    private boolean pause = true;

    @Override public void start() {
        super.start();
        getEventBus().<JsonObject>localConsumer(endpoint)
                .handler(this)
                .exceptionHandler(THROWABLE_HANDLER);
    }

    @Override public void handle(Message<JsonObject> event) {
        JsonObject message = event.body();
        if (pause) {
            refuse(event.body());
        } else {
            try {
                handle(message);
            } catch (Throwable th) {
                handleFallback(message, th);
            }
        }
    }

    /**
     * Returns true if the component is paused.
     *
     * @return true if teh component is paused, otherwise false.
     */
    public boolean isPause() {
        return pause;
    }

    /**
     * Resume message handling.
     */
    public void resume() {
        // Update flag
        pause = false;

        // Don't forget to release because refuse use forward-release mecanism
        release();
    }

    /**
     * Pause message handling.
     */
    public void pause() {
        pause = true;
    }

}