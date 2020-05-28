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

import joptsimple.internal.Strings;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperatorFactory;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@OperationsPerInvocation(value = ContinuousFileReaderOperatorBenchmark.RECORDS_PER_INVOCATION)
public class ContinuousFileReaderOperatorBenchmark extends BenchmarkBase {
    private static final int SPLITS_PER_INVOCATION = 100;
    private static final int LINES_PER_SPLIT = 175_000;
    public static final int RECORDS_PER_INVOCATION = SPLITS_PER_INVOCATION * LINES_PER_SPLIT;

    private static final TimestampedFileInputSplit SPLIT = new TimestampedFileInputSplit(0, 0, new Path("."), 0, 0, new String[]{});
    private static final String LINE = Strings.repeat('0', 10);

    // Source should wait until all elements reach sink. Otherwise, END_OF_INPUT is sent once all splits are emitted.
    // Thus, all subsequent reads in ContinuousFileReaderOperator would be made in CLOSING state in a simple while-true loop (MailboxExecutor.isIdle is always true).
    private static OneShotLatch TARGET_COUNT_REACHED_LATCH = new OneShotLatch();

    public static void main(String[] args)
            throws RunnerException {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ContinuousFileReaderOperatorBenchmark.class.getCanonicalName() + ".*")
                .build();

        new Runner(options).run();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        TARGET_COUNT_REACHED_LATCH.reset();
    }

    @Benchmark
    public void readFileSplit(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        env
                .enableCheckpointing(100)
                .setParallelism(1)
                .addSource(new MockSourceFunction())
                .transform("fileReader", TypeInformation.of(String.class),
                        new ContinuousFileReaderOperatorFactory<>(new MockInputFormat()))
                .addSink(new LimitedSink());

        env.execute();
    }

    private static class MockSourceFunction implements SourceFunction<TimestampedFileInputSplit> {
        private volatile boolean isRunning = true;
        private int count = 0;

        @Override
        public void run(SourceContext<TimestampedFileInputSplit> ctx) {
            while (isRunning && count < SPLITS_PER_INVOCATION) {
                count++;
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(SPLIT);
                }
            }
            while (isRunning) {
                try {
                    TARGET_COUNT_REACHED_LATCH.await(100, TimeUnit.MILLISECONDS);
                    return;
                } catch (InterruptedException e) {
                    if (!isRunning) {
                        Thread.currentThread().interrupt();
                    }
                } catch (TimeoutException e) {
                	// continue waiting
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class MockInputFormat extends FileInputFormat<String> {
        private transient int count = 0;

        @Override
        public boolean reachedEnd() {
            return count >= ContinuousFileReaderOperatorBenchmark.LINES_PER_SPLIT;
        }

        @Override
        public String nextRecord(String s) {
            count++;
            return LINE;
        }

        @Override
        public void open(FileInputSplit fileSplit) {
            count = 0;
            // prevent super from accessing file
        }

        @Override
        public void configure(Configuration parameters) {
            // prevent super from requiring certain settings (input.file.path)
        }
    }

    private static class LimitedSink implements SinkFunction<String> {
        private int count;

        @Override
        public void invoke(String value, Context context) {
        	if (++count == RECORDS_PER_INVOCATION) {
        	    TARGET_COUNT_REACHED_LATCH.trigger();
            }
        }
    }
}
