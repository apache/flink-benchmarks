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
import org.apache.flink.config.ConfigUtil;
import org.apache.flink.config.StateBenchmarkOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyedStateBackend;

import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.state.benchmark.StateBackendBenchmarkUtils.cleanUp;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.mapValues;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.newKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.newKeys;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.randomValueCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.randomValues;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeyCount;
import static org.apache.flink.state.benchmark.StateBenchmarkConstants.setupKeys;

/** Base implementation of the state benchmarks. */
public class StateBenchmarkBase extends BenchmarkBase {
    // TODO: why AtomicInteger?
    protected static AtomicInteger keyIndex;
    protected final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Param({"HEAP", "ROCKSDB", "ROCKSDB_CHANGELOG"})
    protected StateBackendBenchmarkUtils.StateBackendType backendType;

    protected KeyedStateBackend<Long> keyedStateBackend;

    protected KeyedStateBackend<Long> createKeyedStateBackend() throws Exception {
        return createKeyedStateBackend(TtlTimeProvider.DEFAULT);
    }

    protected KeyedStateBackend<Long> createKeyedStateBackend(TtlTimeProvider ttlTimeProvider) throws Exception {
        return StateBackendBenchmarkUtils.createKeyedStateBackend(backendType, createStateDataDir());
    }

    public static File createStateDataDir() throws IOException {
        Configuration benchMarkConfig = ConfigUtil.loadBenchMarkConf();
        String stateDataDirPath = benchMarkConfig.get(StateBenchmarkOptions.STATE_DATA_DIR);
        File dataDir = null;
        if (stateDataDirPath != null) {
            dataDir = new File(stateDataDirPath);
            if (!dataDir.exists()) {
                Files.createDirectories(Paths.get(stateDataDirPath));
            }
        }
        return dataDir;
    }

    private static int getCurrentIndex() {
        int currentIndex = keyIndex.getAndIncrement();
        if (currentIndex == Integer.MAX_VALUE) {
            keyIndex.set(0);
        }
        return currentIndex;
    }

    @TearDown
    public void tearDown() throws IOException {
        cleanUp(keyedStateBackend);
    }

    @State(Scope.Thread)
    public static class KeyValue {
        public long newKey;
        public long setUpKey;
        public long mapKey;
        public double mapValue;
        public long value;
        public List<Long> listValue;

        @Setup(Level.Invocation)
        public void kvSetup() {
            int currentIndex = getCurrentIndex();
            setUpKey = setupKeys.get(currentIndex % setupKeyCount);
            newKey = newKeys.get(currentIndex % newKeyCount);
            mapKey = mapKeys.get(currentIndex % mapKeyCount);
            mapValue = mapValues.get(currentIndex % mapKeyCount);
            value = randomValues.get(currentIndex % randomValueCount);
            // TODO: singletonList is taking 25% of time in mapAdd benchmark... This shouldn't be
            // initiated if benchmark is not using it and for the benchmarks that are using it,
            // this should also be probably somehow avoided.
            listValue =
                    Collections.singletonList(randomValues.get(currentIndex % randomValueCount));
        }

        @TearDown(Level.Invocation)
        public void kvTearDown() {
            listValue = null;
        }
    }
}
