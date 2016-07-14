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
package io.techcode.logbulk.pipeline.input;

import com.zaxxer.nuprocess.NuAbstractProcessHandler;
import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import io.techcode.logbulk.component.ComponentVerticle;
import io.vertx.core.Context;
import io.vertx.core.TimeoutStream;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkState;

/**
 * Exec input pipeline component.
 */
@Slf4j
public class ExecInput extends ComponentVerticle {

    // Current vertx context
    private Context ctx;

    @Override public void start() {
        super.start();

        // Setup processing task
        String command = config.getString("command");
        String interpreter = config.getString("interpreter", "/bin/bash");
        String arguments = config.getString("arguments", "-c");
        int interval = config.getInteger("interval", 1);
        ctx = vertx.getOrCreateContext();

        // Setup periodic task
        TimeoutStream stream = vertx.periodicStream(interval * 1000)
                .handler(h -> {
                    NuProcessBuilder builder = new NuProcessBuilder(interpreter, arguments, command);
                    builder.setProcessListener(new ProcessHandler());
                    builder.start();
                });
        handlePressure(stream);
    }

    @Override public JsonObject config() {
        JsonObject config = super.config();
        checkState(config.getString("command") != null, "The command is required");
        return config;
    }

    /**
     * Process handler.
     */
    private class ProcessHandler extends NuAbstractProcessHandler {

        // NuProcess ref
        private NuProcess process;

        @Override public void onStart(NuProcess process) {
            this.process = process;
        }

        @Override public void onStdout(ByteBuffer buf, boolean close) {
            if (buf.remaining() == 0) return;
            byte[] data = new byte[buf.remaining()];
            buf.get(data);
            ctx.runOnContext(h -> {
                createEvent(new String(data));
                process.closeStdin(true);
            });
        }

    }

}