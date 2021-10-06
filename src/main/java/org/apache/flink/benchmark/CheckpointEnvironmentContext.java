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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;

/**
 * Common context to be used for benchmarking checkpointing time.
 *
 * @see CheckpointingTimeBenchmark
 * @see MultiInputCheckpointingTimeBenchmark
 */
public abstract class CheckpointEnvironmentContext extends FlinkEnvironmentContext {

    public static final int JOB_PARALLELISM = 4;
    public static final MemorySize START_MEMORY_SEGMENT_SIZE = MemorySize.parse("8 kb");
    public static final MemorySize MIN_MEMORY_SEGMENT_SIZE = MemorySize.parse("256 b");
    public static final Duration DEBLOATING_TARGET = Duration.of(300, ChronoUnit.MILLIS);
    public static final int DEBLOATING_STABILIZATION_PERIOD = 2_000;

    public JobID jobID;

    /**
     * Checkpointing configuration to be used in {@link CheckpointingTimeBenchmark} & {@link
     * MultiInputCheckpointingTimeBenchmark}.
     */
    public enum CheckpointMode {
        UNALIGNED(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true);
                    config.set(
                            TaskManagerOptions.MEMORY_SEGMENT_SIZE,
                            CheckpointEnvironmentContext.START_MEMORY_SEGMENT_SIZE);
                    config.set(
                            ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                            Duration.ofMillis(0));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, false);
                    return config;
                }),
        UNALIGNED_1(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, true);
                    config.set(
                            TaskManagerOptions.MEMORY_SEGMENT_SIZE,
                            CheckpointEnvironmentContext.START_MEMORY_SEGMENT_SIZE);
                    config.set(
                            ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT,
                            Duration.ofMillis(1));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, false);
                    return config;
                }),
        ALIGNED(
                config -> {
                    config.set(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
                    config.set(
                            TaskManagerOptions.MEMORY_SEGMENT_SIZE,
                            CheckpointEnvironmentContext.START_MEMORY_SEGMENT_SIZE);
                    config.set(
                            TaskManagerOptions.MIN_MEMORY_SEGMENT_SIZE,
                            CheckpointEnvironmentContext.MIN_MEMORY_SEGMENT_SIZE);
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true);
                    config.set(
                            TaskManagerOptions.BUFFER_DEBLOAT_TARGET,
                            CheckpointEnvironmentContext.DEBLOATING_TARGET);
                    config.set(
                            TaskManagerOptions.BUFFER_DEBLOAT_PERIOD,
                            Duration.of(200, ChronoUnit.MILLIS));
                    config.set(TaskManagerOptions.BUFFER_DEBLOAT_SAMPLES, 5);
                    return config;
                });
        private final Function<Configuration, Configuration> configFunc;

        CheckpointMode(Function<Configuration, Configuration> configFunc) {
            this.configFunc = configFunc;
        }

        public Configuration configure(Configuration config) {
            return configFunc.apply(config);
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env.setParallelism(CheckpointEnvironmentContext.JOB_PARALLELISM);
        env.enableCheckpointing(Long.MAX_VALUE);

        final StreamGraphWithSources streamGraphWithSources = getStreamGraph();

        final JobClient jobClient = env.executeAsync(streamGraphWithSources.getStreamGraph());
        jobID = jobClient.getJobID();
        CommonTestUtils.waitForAllTaskRunning(miniCluster, jobID, false);
        BackpressureUtils.waitForBackpressure(
                jobID, streamGraphWithSources.getSources(), miniCluster.getRestAddress().get());
        if (getSleepPostSetUp() > 0) {
            Thread.sleep(getSleepPostSetUp());
        }
    }

    protected abstract CheckpointMode getMode();

    protected abstract StreamGraphWithSources getStreamGraph();

    protected int getSleepPostSetUp() {
        return getMode() == CheckpointMode.ALIGNED
                ? CheckpointEnvironmentContext.DEBLOATING_STABILIZATION_PERIOD
                : 0;
    }

    @Override
    protected Configuration createConfiguration() {
        return getMode().configure(super.createConfiguration());
    }

    /** A simple wrapper to pass a {@link StreamGraph} along with ids of sources it contains. */
    public static class StreamGraphWithSources {
        private final StreamGraph streamGraph;
        private final List<JobVertexID> sources;

        public StreamGraphWithSources(StreamGraph streamGraph, List<JobVertexID> sources) {
            this.streamGraph = streamGraph;
            this.sources = sources;
        }

        public StreamGraph getStreamGraph() {
            return streamGraph;
        }

        public List<JobVertexID> getSources() {
            return sources;
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
