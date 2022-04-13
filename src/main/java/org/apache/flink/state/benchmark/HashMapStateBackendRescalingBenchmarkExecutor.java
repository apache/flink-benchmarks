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
import org.apache.flink.config.ConfigUtil;
import org.apache.flink.config.StateBenchmarkOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.benchmark.RescalingBenchmarkBuilder;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;
import java.net.URI;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Warmup(iterations = 3)
public class HashMapStateBackendRescalingBenchmarkExecutor extends RescalingBenchmarkBase {
    // numberOfKeys = 1250000, keyLen = 96, valueLen = 128, state size ~= 270MB
    private final int numberOfKeys = 1250000;
    private final int keyLen = 96;

    public static void main(String[] args) throws RunnerException {
        runBenchmark(HashMapStateBackendRescalingBenchmarkExecutor.class);
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // FsStateBackend is deprecated in favor of HashMapStateBackend with setting checkpointStorage.
        HashMapStateBackend stateBackend = new HashMapStateBackend();
        Configuration benchMarkConfig = ConfigUtil.loadBenchMarkConf();
        String stateDataDirPath = benchMarkConfig.getString(StateBenchmarkOptions.STATE_DATA_DIR);
        benchmark =
                new RescalingBenchmarkBuilder<byte[]>()
                        .setMaxParallelism(128)
                        .setParallelismBefore(rescaleType.getParallelismBefore())
                        .setParallelismAfter(rescaleType.getParallelismAfter())
                        .setCheckpointStorageAccess(
                                new FileSystemCheckpointStorage(new URI("file://" + stateDataDirPath), 0)
                                        .createCheckpointStorage(new JobID()))
                        .setStateBackend(stateBackend)
                        .setStreamRecordGenerator(new ByteArrayRecordGenerator(numberOfKeys, keyLen))
                        .setStateProcessFunctionSupplier(TestKeyedFunction::new)
                        .build();
        benchmark.setUp();
    }

    @Setup(Level.Invocation)
    public void setUpPerInvocation() throws Exception {
        benchmark.prepareStateForOperator(rescaleType.getSubtaskIndex());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        benchmark.tearDown();
    }

    @Benchmark
    public void rescaleHeap() throws Exception {
        benchmark.rescale();
    }

    @TearDown(Level.Invocation)
    public void tearDownPerInvocation() throws Exception {
        benchmark.closeOperator();
    }
}
