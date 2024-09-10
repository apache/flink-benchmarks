/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;

@OperationsPerInvocation(value = ProcessingTimerBenchmark.PROCESSING_TIMERS_PER_INVOCATION)
public class ProcessingTimerBenchmark extends BenchmarkBase {

    public static final int PROCESSING_TIMERS_PER_INVOCATION = 150_000;

    private static final int PARALLELISM = 1;

    private static OneShotLatch LATCH = new OneShotLatch();

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + ProcessingTimerBenchmark.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void fireProcessingTimers(FlinkEnvironmentContext context) throws Exception {
        LATCH.reset();
        StreamExecutionEnvironment env = context.env;
        env.setParallelism(PARALLELISM);

        env.addSource(new SingleRecordSource())
                .keyBy(String::hashCode)
                .process(new ProcessingTimerKeyedProcessFunction(PROCESSING_TIMERS_PER_INVOCATION))
                .addSink(new DiscardingSink<>());

        env.execute();
    }

    private static class SingleRecordSource extends RichParallelSourceFunction<String> {

        private Random random;

        public SingleRecordSource() {}

        @Override
        public void open(OpenContext openContext) throws Exception {
            this.random = new Random();
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(String.valueOf(random.nextLong()));
            }

            LATCH.await();
        }

        @Override
        public void cancel() {}
    }

    private static class ProcessingTimerKeyedProcessFunction
            extends KeyedProcessFunction<Integer, String, String> {

        private final long timersPerRecord;
        private long firedTimesCount;

        public ProcessingTimerKeyedProcessFunction(long timersPerRecord) {
            this.timersPerRecord = timersPerRecord;
        }

        @Override
        public void open(OpenContext context) throws Exception {
            this.firedTimesCount = 0;
        }

        @Override
        public void processElement(String s, Context context, Collector<String> collector)
                throws Exception {
            final long currTimestamp = System.currentTimeMillis();
            for (int i = 0; i < timersPerRecord; i++) {
                context.timerService().registerProcessingTimeTimer(currTimestamp - timersPerRecord + i);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            if (++firedTimesCount == timersPerRecord) {
                LATCH.trigger();
            }
        }
    }
}
