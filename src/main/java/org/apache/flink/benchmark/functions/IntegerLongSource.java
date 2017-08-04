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

public class IntegerLongSource extends RichParallelSourceFunction<IntegerLongSource.Record> {
    public static final class Record {
        public final int key;
        public final long value;

        public Record() {
            this(0, 0);
        }

        public Record(int key, long value) {
            this.key = key;
            this.value = value;
        }

        public static Record of(int key, long value) {
            return new Record(key, value);
        }

        public int getKey() {
            return key;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s)", key, value);
        }
    }

    private volatile boolean running = true;
    private int numberOfKeys;
    private long numberOfElements;

    public IntegerLongSource(int numberOfKeys, long numberOfElements) {
        this.numberOfKeys = numberOfKeys;
        this.numberOfElements = numberOfElements;
    }

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {
        long counter = 0;

        while (running && counter < numberOfElements) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collectWithTimestamp(Record.of((int) (counter % numberOfKeys), counter), counter);
                counter++;
            }
        }
        running = false;
    }

    @Override
    public void cancel() {
        running = false;
    }
}