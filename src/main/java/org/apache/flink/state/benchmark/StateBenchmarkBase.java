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

import org.apache.flink.benchmark.BenchmarkBase;
import org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils;
import org.apache.flink.runtime.state.KeyedStateBackend;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.contrib.streaming.state.benchmark.StateBackendBenchmarkUtils.cleanUp;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapValues;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.newKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.newKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.randomValueCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.randomValues;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeys;

/**
 * Base implementation of the state benchmarks.
 */
public class StateBenchmarkBase extends BenchmarkBase {
    KeyedStateBackend<Long> keyedStateBackend;

    @Param({"HEAP", "ROCKSDB"})
    protected StateBackendBenchmarkUtils.StateBackendType backendType;

    final ThreadLocalRandom random = ThreadLocalRandom.current();

    @TearDown
    public void tearDown() throws IOException {
        cleanUp(keyedStateBackend);
    }

    static AtomicInteger keyIndex;

    private static int getCurrentIndex() {
        int currentIndex = keyIndex.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            keyIndex.set(0);
        }
        return currentIndex;
    }

    @State(Scope.Thread)
    public static class KeyValue {
        @Setup(Level.Invocation)
        public void kvSetup() {
            int currentIndex = getCurrentIndex();
            setUpKey = setupKeys.get(currentIndex % setupKeyCount);
            newKey = newKeys.get(currentIndex % newKeyCount);
            mapKey = mapKeys.get(currentIndex % mapKeyCount);
            mapValue = mapValues.get(currentIndex % mapKeyCount);
            value = randomValues.get(currentIndex % randomValueCount);
            listValue = Collections.singletonList(randomValues.get(currentIndex % randomValueCount));
        }

        @TearDown(Level.Invocation)
        public void kvTearDown() {
            listValue = null;
        }

        long newKey;
        long setUpKey;
        long mapKey;
        double mapValue;
        long value;
        List<Long> listValue;
    }
}
