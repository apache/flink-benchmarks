/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark.functions;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class LongSource extends RichParallelSourceFunction<Long> {

    private volatile boolean running = true;
    private long maxValue;

    public LongSource(long maxValue) {
        this.maxValue = maxValue;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long counter = 0;

        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(counter);
                counter++;
                if (counter >= maxValue) {
                    cancel();
                }
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
