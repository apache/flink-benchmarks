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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.state.benchmark.StateBenchmarkBase.createStateDataDir;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

/**
 * Measures how long a keyed operator takes to open after a restore, when all of its timers fired
 * before the checkpoint.
 *
 * <p>A fired timer stays in the RocksDB timer column family as a deletion marker until a compaction
 * removes it. Opening the operator creates the timer service, which looks up the first timer of
 * every key group. With no live timer left, each lookup has to skip deletion markers, so the time
 * depends on how many of them a single lookup visits.
 */
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Warmup(iterations = 3)
public class RocksdbTimerServiceInitBenchmark extends BenchmarkBase {

    private static final int FIRED_TIMERS = 100_000;

    /**
     * Large enough that the checkpoint holds the deletion markers of all {@link #FIRED_TIMERS}
     * timers in a single RocksDB file.
     */
    private static final long MANAGED_MEMORY_SIZE = 256 * 1024 * 1024;

    /**
     * The number of key groups of the restored subtask, which runs with parallelism 1. With a
     * single key group, opening the operator does not read the event-time timers, so the value 1
     * only measures the rest of the open.
     */
    @Param({"128", "4096"})
    private int maxParallelism;

    private File trialDirectory;

    private CheckpointStorageAccess checkpointStorageAccess;

    private OperatorSubtaskState firedTimersSnapshot;

    private MockEnvironment environment;

    private KeyedOneInputStreamOperatorTestHarness<Long, Long, Void> harness;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + RocksdbTimerServiceInitBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(options).run();
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        trialDirectory =
                StateBackendBenchmarkUtils.prepareDirectory("timerDb", createStateDataDir());
        try {
            checkpointStorageAccess =
                    new FileSystemCheckpointStorage(new File(trialDirectory, "checkpoints").toURI())
                            .createCheckpointStorage(new JobID());
            firedTimersSnapshot = createFiredTimersSnapshot();
        } catch (Throwable t) {
            FileUtils.deleteDirectoryQuietly(trialDirectory);
            throw t;
        }
    }

    private OperatorSubtaskState createFiredTimersSnapshot() throws Exception {
        try (MockEnvironment writerEnvironment = createEnvironment();
                KeyedOneInputStreamOperatorTestHarness<Long, Long, Void> writer =
                        createHarness(writerEnvironment)) {
            writer.open();
            for (long i = 0; i < FIRED_TIMERS; i++) {
                writer.processElement(i, i);
            }
            writer.processWatermark(FIRED_TIMERS);
            Preconditions.checkState(writer.numEventTimeTimers() == 0, "All timers must fire.");
            // The incremental checkpoint keeps the deletion markers of the fired timers, because
            // it contains the RocksDB files as they are.
            return writer.snapshot(1L, 1L);
        }
    }

    @Setup(Level.Invocation)
    public void setUpPerInvocation() throws Exception {
        try {
            environment = createEnvironment();
            harness = createHarness(environment);
            harness.initializeState(firedTimersSnapshot);
        } catch (Throwable t) {
            releaseAfterFailure(t);
            throw t;
        }
    }

    @Benchmark
    public void timerServiceInit() throws Exception {
        try {
            harness.open();
        } catch (Throwable t) {
            releaseAfterFailure(t);
            throw t;
        }
    }

    @TearDown(Level.Invocation)
    public void tearDownPerInvocation() throws Exception {
        try {
            closeHarnessAndEnvironment();
        } catch (Throwable t) {
            releaseAfterFailure(t);
            throw t;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        // The snapshot only refers to files in this directory.
        FileUtils.deleteDirectory(trialDirectory);
    }

    /**
     * Releases everything the trial holds. JMH calls no teardown method after a setup, benchmark or
     * teardown method failed, so the failing method has to do it.
     */
    private void releaseAfterFailure(Throwable failure) {
        try {
            closeHarnessAndEnvironment();
        } catch (Throwable t) {
            failure.addSuppressed(t);
        }
        FileUtils.deleteDirectoryQuietly(trialDirectory);
    }

    private void closeHarnessAndEnvironment() throws Exception {
        // Closes the environment even if closing the harness fails.
        try (MockEnvironment ignored = environment) {
            if (harness != null) {
                harness.close();
            }
        } finally {
            harness = null;
            environment = null;
        }
    }

    private MockEnvironment createEnvironment() {
        final MockEnvironment mockEnvironment =
                new MockEnvironmentBuilder()
                        .setTaskName("TimerTask")
                        .setManagedMemorySize(MANAGED_MEMORY_SIZE)
                        .setMaxParallelism(maxParallelism)
                        .setParallelism(1)
                        .setSubtaskIndex(0)
                        .build();
        mockEnvironment.setCheckpointStorageAccess(checkpointStorageAccess);
        return mockEnvironment;
    }

    private KeyedOneInputStreamOperatorTestHarness<Long, Long, Void> createHarness(
            MockEnvironment mockEnvironment) throws Exception {
        final KeyedOneInputStreamOperatorTestHarness<Long, Long, Void> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new TimerPerRecordFunction()),
                        value -> value,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        mockEnvironment);
        final EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(true);
        stateBackend.setDbStoragePath(trialDirectory.getAbsolutePath());
        // Fired timers leave deletion markers behind only if RocksDB stores the timers.
        stateBackend.setPriorityQueueStateType(
                EmbeddedRocksDBStateBackend.PriorityQueueStateType.ROCKSDB);
        testHarness.setStateBackend(stateBackend);
        testHarness.setup();
        return testHarness;
    }

    /** Registers one event-time timer for every record, at the timestamp of the record. */
    private static class TimerPerRecordFunction extends KeyedProcessFunction<Long, Long, Void> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(Long value, Context ctx, Collector<Void> out) {
            ctx.timerService().registerEventTimeTimer(ctx.timestamp());
        }
    }
}
