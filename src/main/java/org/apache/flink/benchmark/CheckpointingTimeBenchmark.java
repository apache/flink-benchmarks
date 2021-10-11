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

package org.apache.flink.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.benchmark.operators.RecordSource;
import org.apache.flink.benchmark.operators.RecordSource.Record;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureHeaders;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

/**
 * The test verifies that the debloating kicks in and properly downsizes buffers. In the end the
 * checkpoint should take ~2(number of rebalance) * DEBLOATING_TARGET.
 *
 * <p>Some info about the chosen numbers:
 *
 * <ul>
 *   <li>The minimal memory segment size is decreased (256b) so that the scaling possibility is
 *       higher. Memory segments start with 8kb
 *   <li>A memory segment of the minimal size fits ~9 records (of size 29b), each record takes ~200ns
 *       to be processed by the sink
 *   <li>We have 2 (exclusive buffers) * 4 (parallelism) + 8 floating = 64 buffers per gate, with
 *       300 ms debloating target and ~200ns/record processing speed, we can buffer 1500/64 = ~24
 *       records in a buffer after debloating which means the size of a buffer (24 * 29 = 696) is slightly above the
 *       minimal memory segment size.
 *   <li>The buffer debloating target of 300ms means a checkpoint should take ~2(number of
 *       exchanges)*300ms=~600ms
 * </ul>
 */
@OutputTimeUnit(SECONDS)
public class CheckpointingTimeBenchmark extends BenchmarkBase {
    public static final int JOB_PARALLELISM = 4;
    public static final MemorySize START_MEMORY_SEGMENT_SIZE = MemorySize.parse("8 kb");
    public static final MemorySize MIN_MEMORY_SEGMENT_SIZE = MemorySize.parse("256 b");
    public static final Duration DEBLOATING_TARGET = Duration.of(300, ChronoUnit.MILLIS);
    public static final MemorySize DEBLOATING_RECORD_SIZE = MemorySize.parse("1b");
    public static final MemorySize UNALIGNED_RECORD_SIZE = MemorySize.parse("1kb");
    public static final int DEBLOATING_STABILIZATION_PERIOD = 2_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(CheckpointingTimeBenchmark.class.getCanonicalName())
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public void checkpointSingleInput(CheckpointEnvironmentContext context) throws Exception {
        final CompletableFuture<String> checkpoint =
                context.miniCluster.triggerCheckpoint(context.jobID);
        checkpoint.get();
    }

    public enum CheckpointMode {
        UNALIGNED(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true);
                    config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, START_MEMORY_SEGMENT_SIZE);
                    config.set(
                            ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                            Duration.ofMillis(0));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, false);
                    return config;
                },
                0,
                UNALIGNED_RECORD_SIZE),
        UNALIGNED_1(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true);
                    config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, START_MEMORY_SEGMENT_SIZE);
                    config.set(
                            ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                            Duration.ofMillis(1));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, false);
                    return config;
                },
                0,
                UNALIGNED_RECORD_SIZE),
        ALIGNED(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
                    config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, START_MEMORY_SEGMENT_SIZE);
                    config.set(TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE, MIN_MEMORY_SEGMENT_SIZE);
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true);
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_TARGET, DEBLOATING_TARGET);
                    config.set(
                            TaskManagerOptions.BUFFER_DEBLOAT_PERIOD,
                            Duration.of(200, ChronoUnit.MILLIS));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_SAMPLES, 5);
                    return config;
                },
                DEBLOATING_STABILIZATION_PERIOD,
                DEBLOATING_RECORD_SIZE);

        private final Function<Configuration, Configuration> configFunc;
        private final int sleepPostSetUp;
        private final MemorySize recordSize;

        CheckpointMode(
                Function<Configuration, Configuration> configFunc,
                int sleepPostSetUp,
                MemorySize recordSize) {
            this.configFunc = configFunc;
            this.sleepPostSetUp = sleepPostSetUp;
            this.recordSize = recordSize;
        }

        public Configuration configure(Configuration config) {
            return configFunc.apply(config);
        }

        public MemorySize getRecordSize() {
            return recordSize;
        }

        public int getSleepPostSetUp() {
            return sleepPostSetUp;
        }
    }

    @State(Scope.Thread)
    public static class CheckpointEnvironmentContext extends FlinkEnvironmentContext {
        public JobID jobID;

        @Param({"ALIGNED", "UNALIGNED", "UNALIGNED_1"})
        public CheckpointMode mode;

        @Override
        public void setUp() throws Exception {
            super.setUp();
            env.setParallelism(JOB_PARALLELISM);
            env.enableCheckpointing(Long.MAX_VALUE);

            DataStreamSource<Record> source =
                    env.fromSource(
                            new RecordSource(
                                    Integer.MAX_VALUE, (int) mode.getRecordSize().getBytes()),
                            noWatermarks(),
                            RecordSource.class.getName());

            source.slotSharingGroup("source")
                    .rebalance()
                    .map((MapFunction<Record, Record>) value -> value)
                    .slotSharingGroup("map")
                    .rebalance()
                    .addSink(new SlowDiscardSink<>())
                    .slotSharingGroup("sink");

            final JobVertexID sourceId = extractSourceId();
            final JobClient jobClient = env.executeAsync();
            jobID = jobClient.getJobID();
            CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);
            waitForBackpressure(jobID, sourceId);
            if (mode.getSleepPostSetUp() > 0) {
                Thread.sleep(mode.getSleepPostSetUp());
            }
        }

        private JobVertexID extractSourceId() {
            return env.getStreamGraph(false)
                    .getJobGraph()
                    .getVerticesSortedTopologicallyFromSources()
                    .get(0)
                    .getID();
        }

        private void waitForBackpressure(JobID jobID, JobVertexID sourceId) throws Exception {
            final RestClient restClient =
                    new RestClient(
                            new UnmodifiableConfiguration(new Configuration()),
                            Executors.newSingleThreadScheduledExecutor(
                                    new ExecutorThreadFactory("Flink-RestClient-IO")));
            final URI restAddress = miniCluster.getRestAddress().get();
            final JobVertexMessageParameters metricsParameters = new JobVertexMessageParameters();
            metricsParameters.jobPathParameter.resolve(jobID);
            metricsParameters.jobVertexIdPathParameter.resolve(sourceId);
            JobVertexBackPressureInfo responseBody;
            Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
            do {
                responseBody =
                        restClient
                                .sendRequest(
                                        restAddress.getHost(),
                                        restAddress.getPort(),
                                        JobVertexBackPressureHeaders.getInstance(),
                                        metricsParameters,
                                        EmptyRequestBody.getInstance())
                                .get();
            } while (responseBody.getBackpressureLevel()
                            != JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH
                    && deadline.hasTimeLeft());
            if (responseBody.getBackpressureLevel()
                    != JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH) {
                throw new FlinkRuntimeException(
                        "Could not trigger backpressure for the job in given time.");
            }
        }

        @Override
        protected Configuration createConfiguration() {
            return mode.configure(super.createConfiguration());
        }

        @Override
        protected int getNumberOfTaskManagers() {
            return 3 * JOB_PARALLELISM;
        }

        @Override
        protected int getNumberOfSlotsPerTaskManager() {
            return 1;
        }
    }

    /**
     * The custom sink for processing records slowly to cause accumulate in-flight buffers even back
     * pressure.
     */
    public static class SlowDiscardSink<T> implements SinkFunction<T> {
        @Override
        public void invoke(T value, Context context) {
            final long startTime = System.nanoTime();
            while (System.nanoTime() - startTime < 200_000) {}
        }
    }
}
