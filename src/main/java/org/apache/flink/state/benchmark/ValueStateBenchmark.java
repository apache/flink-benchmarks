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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.createKeyedStateBackend;
import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.getValueState;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;

/**
 * Implementation for listValue state benchmark testing.
 */
public class ValueStateBenchmark extends StateBenchmarkBase {
    private ValueState<Long> valueState;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + ValueStateBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setUp() throws Exception {
        keyedStateBackend = createKeyedStateBackend(backendType);
        valueState = getValueState(
                keyedStateBackend,
                new ValueStateDescriptor<>("kvState", Long.class));
        for (int i = 0; i < setupKeyCount; ++i) {
            keyedStateBackend.setCurrentKey((long) i);
            valueState.update(random.nextLong());
        }
        keyIndex = new AtomicInteger();
    }

    @Benchmark
    public void valueUpdate(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        valueState.update(keyValue.value);
    }

    @Benchmark
    public void valueAdd(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.newKey);
        valueState.update(keyValue.value);
    }

    @Benchmark
    public Long valueGet(KeyValue keyValue) throws IOException {
        keyedStateBackend.setCurrentKey(keyValue.setUpKey);
        return valueState.value();
    }
}
